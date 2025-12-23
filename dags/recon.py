from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from dbfread import DBF  # type: ignore
from pymongo import MongoClient
import sys
import os
import uuid


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.sftp_client import SFTPClient

CAMS_DBF_PATH = "/opt/airflow/sftp_data/downloads/cams.dbf"
KARVEY_DBF_PATH = "/opt/airflow/sftp_data/downloads/karvey.dbf"
BATCH_SIZE = 2500
MONGO_URI = "mongodb://host.docker.internal:27017"
DB_NAME = "banking_demo"
TRANSACTIONS_COLLECTION = "wealth_pulse_transactions"
RECONCILIATION_COLLECTION = "reconciliation_results"


@task
def download_from_sftp():
    client = SFTPClient(
        host="sftp-server",
        port=22,
        username="testuser",
        password="testpass"
    )

    if client.connect():
        client.download_dbf_files()
        client.disconnect()


@task
def extract_dbf_date():
    errors = []
    try:
        print(f"Attempting to read date from CAMS DBF: {CAMS_DBF_PATH}")
        cams_table = DBF(CAMS_DBF_PATH, load=False)

        for record in cams_table:
            asset_date_str = record.get('ASSET_DATE', '').strip()
            if not asset_date_str:
                raise ValueError("ASSET_DATE field is empty in CAMS DBF")

            try:
                transaction_date = datetime.strptime(asset_date_str, "%d-%m-%Y")
                print(f"✓ Extracted date from CAMS: {transaction_date.strftime('%Y-%m-%d')}")
                return transaction_date
            except ValueError as e:
                raise ValueError(
                    f"Invalid date format in CAMS ASSET_DATE: '{asset_date_str}'. Expected DD-MM-YYYY"
                ) from e

    except FileNotFoundError as e:
        error_msg = f"CAMS DBF file not found at {CAMS_DBF_PATH}"
        print(f"⚠ {error_msg}", e)
        errors.append(error_msg)
    except Exception as e:
        error_msg = f"Error reading CAMS DBF: {str(e)}"
        print(f"⚠ {error_msg}")
        errors.append(error_msg)

    # Try Karvy if CAMS failed
    try:
        print(f"Attempting to read date from Karvy DBF: {KARVEY_DBF_PATH}")
        karvy_table = DBF(KARVEY_DBF_PATH, load=False)

        # Read first record to get date
        for record in karvy_table:
            trdate_str = record.get('TRDATE', '').strip()
            if not trdate_str:
                raise ValueError("TRDATE field is empty in Karvy DBF")

            # Parse date format: DD-MM-YYYY
            try:
                transaction_date = datetime.strptime(trdate_str, "%d-%m-%Y")
                print(f"✓ Extracted date from Karvy: {transaction_date.strftime('%Y-%m-%d')}")
                return transaction_date
            except ValueError as e:
                raise ValueError(f"Invalid date format in Karvy TRDATE: '{trdate_str}'. Expected DD-MM-YYYY") from e

    except FileNotFoundError as e:
        error_msg = f"Karvy DBF file not found at {KARVEY_DBF_PATH}"
        print(f"⚠ {error_msg}", e)
        errors.append(error_msg)

    except Exception as e:
        error_msg = f"Error reading Karvy DBF: {str(e)}"
        print(f"⚠ {error_msg}")
        errors.append(error_msg)

    # If both failed, raise error to fail the task
    error_summary = "\n".join([f"  - {err}" for err in errors])
    raise FileNotFoundError(
        f"✗ FAILED: Could not extract date from any DBF file.\n"
        f"Errors encountered:\n{error_summary}\n\n"
        f"Please ensure DBF files are present and readable:\n"
        f"  - CAMS: {CAMS_DBF_PATH}\n"
        f"  - Karvy: {KARVEY_DBF_PATH}"
    )


@task
def aggregate_mongodb_transactions(transaction_date=None):
    """
    Aggregate MongoDB transactions, optionally filtered by date.

    Args:
        transaction_date: datetime object to filter transactions (from DBF file date)
    """
    client = MongoClient("mongodb://host.docker.internal:27017")
    db = client["banking_demo"]
    collection = db["wealth_pulse_transactions"]

    aggregations_map = {}
    total_transactions = 0

    # Build query filter
    query_filter = {}
    if transaction_date:
        # Filter transactions up to and including the transaction_date
        query_filter["transaction_date"] = {"$lte": transaction_date}
        print(f"Filtering MongoDB transactions up to date: {transaction_date.strftime('%Y-%m-%d')}")
    else:
        print("No date filter applied - processing all MongoDB transactions")

    for txn in collection.find(query_filter):
        folio = txn["folio_no"]
        scheme = txn["scheme_name"]
        product_code = txn["product_code"]
        qty = txn["units"]
        txn_type = txn["transaction_type"]

        key = f"{product_code}|{folio}|{scheme}"

        if key not in aggregations_map:
            aggregations_map[key] = 0.0

        if txn_type == "BUY":
            aggregations_map[key] += qty
        elif txn_type == "SELL":
            aggregations_map[key] -= qty

        total_transactions += 1

    for key in aggregations_map:
        aggregations_map[key] = round(aggregations_map[key], 3)

    client.close()

    print(f"✓ Processed {total_transactions} transactions")
    print(f"✓ Created {len(aggregations_map)} unique combinations")
    print("✓ SUCCESS: Aggregation completed!")
    return aggregations_map


@task
def reconcile_dbfs(aggregations_map):
    print(f"DEBUG: Received aggregations_map with {len(aggregations_map)} entries")

    matched = []
    mismatched = []
    dbf_only = []
    processed_mongo_keys = set()

    def process_dbf_record(key, dbf_units, source):
        if key in aggregations_map:
            if key in processed_mongo_keys:
                print(f"WARNING: Duplicate key found in {source}: {key}")
                return

            print(f"Matched Key from {source}: {key}")
            mongo_units = round(aggregations_map[key], 3)
            dbf_units_rounded = round(dbf_units, 3)

            record_data = {
                'source': source,
                'product_code': key[0],
                'folio': key[1],
                'scheme': key[2],
                'mongo_units': mongo_units,
                'dbf_units': dbf_units_rounded,
            }

            if mongo_units == dbf_units_rounded:
                record_data['status'] = 'MATCHED'
                matched.append(record_data)
            else:
                record_data['difference'] = round(mongo_units - dbf_units_rounded, 3)
                record_data['status'] = 'MISMATCHED'
                mismatched.append(record_data)

            processed_mongo_keys.add(key)
        else:
            dbf_only.append({
                'source': source,
                'product_code': key[0],
                'folio': key[1],
                'scheme': key[2],
                'dbf_units': round(dbf_units, 3),
                'status': 'DBF_ONLY'
            })

    # Process CAMS DBF
    print("Processing CAMS DBF...")
    cams_table = DBF(CAMS_DBF_PATH, load=False)
    cams_count = 0

    for record in cams_table:
        product = record['PRODUCT'].strip()
        folio = record['FOLIO'].strip()
        scheme = record['SCHEME_NAM'].strip()
        dbf_units = float(record['UNITS'])
        key = f"{product}|{folio}|{scheme}"
        process_dbf_record(key, dbf_units, 'CAMS')
        cams_count += 1

    print(f"✓ Processed {cams_count} CAMS records")

    # Process Karvy DBF
    print("Processing Karvy DBF...")
    karvy_table = DBF(KARVEY_DBF_PATH, load=False)
    karvy_count = 0

    for record in karvy_table:
        product = record['PRCODE'].strip()
        folio = str(record['ACNO']).strip()
        scheme = record['FUNDDESC'].strip()
        dbf_units = float(record['BALUNITS'])
        key = f"{product}|{folio}|{scheme}"
        process_dbf_record(key, dbf_units, 'KARVY')
        karvy_count += 1

    print(f"✓ Processed {karvy_count} Karvy records")

    # Find MongoDB-only records
    mongo_only = []
    for key, units in aggregations_map.items():
        if key not in processed_mongo_keys:
            mongo_only.append({
                'product_code': key[0],
                'folio': key[1],
                'scheme': key[2],
                'mongo_units': round(units, 3),
                'status': 'MONGO_ONLY'
            })

    print("\n" + "=" * 70)
    print("RECONCILIATION SUMMARY")
    print("=" * 70)
    print(f"✓ Matched:        {len(matched)}")
    print(f"⚠ Mismatched:     {len(mismatched)}")
    print(f"📂 DBF Only:      {len(dbf_only)}")
    print(f"💾 MongoDB Only:  {len(mongo_only)}")
    print("=" * 70)

    return {
        'matched': matched,
        'mismatched': mismatched,
        'dbf_only': dbf_only,
        'mongo_only': mongo_only,
        'summary': {
            'matched_count': len(matched),
            'mismatched_count': len(mismatched),
            'dbf_only_count': len(dbf_only),
            'mongo_only_count': len(mongo_only),
        }
    }


@task
def store_reconciliation_results(reconciliation_data):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[RECONCILIATION_COLLECTION]

    collection.create_index([
        ("reconciliation_run_id", 1),
        ("status", 1)
    ])
    collection.create_index([
        ("folio", 1),
        ("product_code", 1),
        ("scheme", 1)
    ])
    collection.create_index([("reconciliation_date", -1)])
    collection.create_index([("status", 1)])

    run_id = str(uuid.uuid4())
    reconciliation_date = datetime.now()
    created_at = datetime.now()

    print(f"Reconciliation Run ID: {run_id}")
    print(f"Reconciliation Date: {reconciliation_date}")

    all_records = []

    # Store MATCHED records
    for record in reconciliation_data['matched']:
        doc = {
            'reconciliation_run_id': run_id,
            'reconciliation_date': reconciliation_date,
            'folio': record['folio'],
            'scheme': record['scheme'],
            'product_code': record['product_code'],
            'source': record['source'],
            'status': record['status'],
            'mongo_units': record['mongo_units'],
            'dbf_units': record['dbf_units'],
            'difference': 0.0,
            'created_at': created_at
        }
        all_records.append(doc)

    # Store MISMATCHED records
    for record in reconciliation_data['mismatched']:
        doc = {
            'reconciliation_run_id': run_id,
            'reconciliation_date': reconciliation_date,
            'folio': record['folio'],
            'scheme': record['scheme'],
            'product_code': record['product_code'],
            'source': record['source'],
            'status': record['status'],
            'mongo_units': record['mongo_units'],
            'dbf_units': record['dbf_units'],
            'difference': record['difference'],
            'created_at': created_at
        }
        all_records.append(doc)

    # Store DBF_ONLY records (Available only in DBF)
    for record in reconciliation_data['dbf_only']:
        doc = {
            'reconciliation_run_id': run_id,
            'reconciliation_date': reconciliation_date,
            'folio': record['folio'],
            'scheme': record['scheme'],
            'product_code': record['product_code'],
            'source': record['source'],
            'status': 'AVAILABLE_ONLY_IN_DBF',
            'mongo_units': None,
            'dbf_units': record['dbf_units'],
            'difference': None,
            'created_at': created_at
        }
        all_records.append(doc)

    # Store MONGO_ONLY records (Available only in WP_MONGO)
    for record in reconciliation_data['mongo_only']:
        doc = {
            'reconciliation_run_id': run_id,
            'reconciliation_date': reconciliation_date,
            'folio': record['folio'],
            'scheme': record['scheme'],
            'product_code': record['product_code'],
            'source': 'WP_MONGO',
            'status': 'AVAILABLE_ONLY_IN_WP_MONGO',
            'mongo_units': record['mongo_units'],
            'dbf_units': None,
            'difference': None,
            'created_at': created_at
        }
        all_records.append(doc)

    if all_records:
        result = collection.insert_many(all_records)
        inserted_count = len(result.inserted_ids)
        print(f"✓ Inserted {inserted_count} records into "
              f"{RECONCILIATION_COLLECTION}")
    else:
        inserted_count = 0
        print("No records to insert")

    client.close()

    print("\n" + "=" * 70)
    print("DATABASE STORAGE SUMMARY")
    print("=" * 70)
    print(f"Run ID:                      {run_id}")
    print(f"Total Records Inserted:      {inserted_count}")
    print(f"  - Matched:                 {len(reconciliation_data['matched'])}")
    print(f"  - Mismatched:              {len(reconciliation_data['mismatched'])}")
    print(f"  - Available Only in DBF:   {len(reconciliation_data['dbf_only'])}")
    print(f"  - Available Only in Mongo: {len(reconciliation_data['mongo_only'])}")
    print(f"Collection:                  {DB_NAME}.{RECONCILIATION_COLLECTION}")
    print("=" * 70)

    return {
        'run_id': run_id,
        'inserted_count': inserted_count,
        'reconciliation_date': reconciliation_date.isoformat()
    }


with DAG(
    dag_id="aum-recon-test",
    default_args={
        "owner": "batch_processing",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    download_task = download_from_sftp()
    dbf_date = extract_dbf_date()
    agg_map = aggregate_mongodb_transactions(dbf_date)
    recon_result = reconcile_dbfs(agg_map)
    store_task = store_reconciliation_results(recon_result)

    download_task >> dbf_date >> agg_map >> recon_result >> store_task
