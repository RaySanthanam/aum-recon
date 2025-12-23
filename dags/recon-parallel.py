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
    # Try CAMS first
    try:
        cams_table = DBF(CAMS_DBF_PATH, load=False)
        for record in cams_table:
            asset_date = record.get("ASSET_DATE", "").strip()
            return datetime.strptime(asset_date, "%d-%m-%Y")
    except Exception:
        pass

    # Fallback to KARVY
    try:
        karvy_table = DBF(KARVEY_DBF_PATH, load=False)
        for record in karvy_table:
            trdate = record.get("TRDATE", "").strip()
            return datetime.strptime(trdate, "%d-%m-%Y")
    except Exception as e:
        raise RuntimeError("Failed to extract date from DBF files") from e


@task
def aggregate_mongodb_transactions(transaction_date):
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][TRANSACTIONS_COLLECTION]

    aggregations = {}

    query = {"transaction_date": {"$lte": transaction_date}}

    for txn in collection.find(query):
        key = f"{txn['product_code']}|{txn['folio_no']}|{txn['scheme_name']}"
        aggregations.setdefault(key, 0.0)

        if txn["transaction_type"] == "BUY":
            aggregations[key] += txn["units"]
        elif txn["transaction_type"] == "SELL":
            aggregations[key] -= txn["units"]

    client.close()

    return {k: round(v, 3) for k, v in aggregations.items()}


@task
def reconcile_cams(aggregations_map):
    matched, mismatched, dbf_only = [], [], []
    processed_keys = set()

    cams_table = DBF(CAMS_DBF_PATH, load=False)

    for record in cams_table:
        product = record["PRODUCT"].strip()
        folio = record["FOLIO"].strip()
        scheme = record["SCHEME_NAM"].strip()
        dbf_units = round(float(record["UNITS"]), 3)

        key = f"{product}|{folio}|{scheme}"

        if key in aggregations_map:
            mongo_units = aggregations_map[key]
            processed_keys.add(key)

            if mongo_units == dbf_units:
                matched.append({
                    "source": "CAMS",
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "status": "MATCHED",
                })
            else:
                mismatched.append({
                    "source": "CAMS",
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "difference": round(mongo_units - dbf_units, 3),
                    "status": "MISMATCHED",
                })
        else:
            dbf_only.append({
                "source": "CAMS",
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "dbf_units": dbf_units,
                "status": "DBF_ONLY",
            })

    return {
        "matched": matched,
        "mismatched": mismatched,
        "dbf_only": dbf_only,
        "processed_keys": list(processed_keys),
    }


@task
def reconcile_karvy(aggregations_map):
    matched, mismatched, dbf_only = [], [], []
    processed_keys = set()

    karvy_table = DBF(KARVEY_DBF_PATH, load=False)

    for record in karvy_table:
        product = record["PRCODE"].strip()
        folio = str(record["ACNO"]).strip()
        scheme = record["FUNDDESC"].strip()
        dbf_units = round(float(record["BALUNITS"]), 3)

        key = f"{product}|{folio}|{scheme}"

        if key in aggregations_map:
            mongo_units = aggregations_map[key]
            processed_keys.add(key)

            if mongo_units == dbf_units:
                matched.append({
                    "source": "KARVY",
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "status": "MATCHED",
                })
            else:
                mismatched.append({
                    "source": "KARVY",
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "difference": round(mongo_units - dbf_units, 3),
                    "status": "MISMATCHED",
                })
        else:
            dbf_only.append({
                "source": "KARVY",
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "dbf_units": dbf_units,
                "status": "DBF_ONLY",
            })

    return {
        "matched": matched,
        "mismatched": mismatched,
        "dbf_only": dbf_only,
        "processed_keys": list(processed_keys),
    }


@task
def merge_results(cams, karvy, aggregations_map):
    processed = set(cams["processed_keys"] + karvy["processed_keys"])

    mongo_only = []
    for key, units in aggregations_map.items():
        if key not in processed:
            product, folio, scheme = key.split("|")
            mongo_only.append({
                "source": "WP_MONGO",
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "mongo_units": units,
                "status": "MONGO_ONLY",
            })

    return {
        "matched": cams["matched"] + karvy["matched"],
        "mismatched": cams["mismatched"] + karvy["mismatched"],
        "dbf_only": cams["dbf_only"] + karvy["dbf_only"],
        "mongo_only": mongo_only,
    }


@task
def store_reconciliation_results(data):
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][RECONCILIATION_COLLECTION]

    run_id = str(uuid.uuid4())
    now = datetime.now()

    records = []
    for group in ["matched", "mismatched", "dbf_only", "mongo_only"]:
        for rec in data[group]:
            rec.update({
                "reconciliation_run_id": run_id,
                "reconciliation_date": now,
                "created_at": now,
            })
            records.append(rec)

    if records:
        collection.insert_many(records)

    client.close()

    # Print summary
    print("\n" + "=" * 70)
    print("RECONCILIATION SUMMARY")
    print("=" * 70)
    print(f"Run ID:                      {run_id}")
    print(f"Total Records Inserted:      {len(records)}")
    print(f"  - Matched:                 {len(data['matched'])}")
    print(f"  - Mismatched:              {len(data['mismatched'])}")
    print(f"  - Available Only in DBF:   {len(data['dbf_only'])}")
    print(f"  - Available Only in Mongo: {len(data['mongo_only'])}")
    print(f"Collection:                  {DB_NAME}.{RECONCILIATION_COLLECTION}")
    print("=" * 70)

    return {"run_id": run_id, "total_records": len(records)}


with DAG(
    dag_id="aum-recon-parallel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "batch_processing",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
) as dag:

    download = download_from_sftp()
    dbf_date = extract_dbf_date()
    agg = aggregate_mongodb_transactions(dbf_date)

    cams = reconcile_cams(agg)
    karvy = reconcile_karvy(agg)

    merged = merge_results(cams, karvy, agg)
    store = store_reconciliation_results(merged)

    download >> dbf_date >> agg
    agg >> [cams, karvy]
    [cams, karvy] >> merged >> store
