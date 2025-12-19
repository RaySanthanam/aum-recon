from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from dbfread import DBF  # type: ignore
from pymongo import MongoClient

CAMS_DBF_PATH = "/opt/airflow/sample_data/CAMS_SMALL_TEST.dbf"
KARVEY_DBF_PATH = "/opt/airflow/sample_data/W1C1927.dbf"
BATCH_SIZE = 2500


@task
def aggregate_mongodb_transactions():
    client = MongoClient("mongodb://host.docker.internal:27017")
    db = client["banking_demo"]
    collection = db["wealth_pulse_transactions"]

    aggregations_map = {}
    total_transactions = 0

    for txn in collection.find():
        folio = txn["folio_no"]
        scheme = txn["scheme_name"]
        product_code = txn["scheme_code"]
        qty = txn["units"]
        txn_type = txn["transaction_type"]

        key = (product_code, folio, scheme)
        print(key)
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

    # serializable map.
    serializable_map = {
        f"{key[0]}|{key[1]}|{key[2]}": value
        for key, value in aggregations_map.items()
    }

    print(f"✓ Processed {total_transactions} transactions")
    print(f"✓ Created {len(aggregations_map)} unique combinations")
    print("✓ SUCCESS: Aggregation completed!")
    print(aggregations_map)
    return serializable_map


@task
def reconcile_dbfs(aggregations_map):
    print(f"DEBUG: Received aggregations_map with {len(aggregations_map)} entries")

    tuple_map = {}
    for key_str, value in aggregations_map.items():
        parts = key_str.split('|')
        tuple_key = (parts[0], parts[1], parts[2])
        tuple_map[tuple_key] = value

    aggregations_map = tuple_map

    if len(aggregations_map) > 0:
        first_key = list(aggregations_map.keys())[0]
        print(f"DEBUG: First key sample: {first_key} = {aggregations_map[first_key]}")

    print("Processing CAMS DBF...")
    cams_table = DBF(CAMS_DBF_PATH, load=False)
    cams_count = 0

    matched = []
    mismatched = []
    dbf_only = []

    processed_mongo_keys = set()

    for record in cams_table:
        product = record['PRODUCT'].strip()
        folio = record['FOLIO'].strip()
        scheme = record['SCHEME_NAM'].strip()
        dbf_units = float(record['UNITS'])

        key = (product, folio, scheme)
        print(key)
        if key in aggregations_map:
            print("Matched Key", key)
            mongo_units = round(aggregations_map[key], 3)
            dbf_units_rounded = round(dbf_units, 3)

            if abs(mongo_units - dbf_units_rounded) < 0.01:
                matched.append({
                    'source': 'CAMS',
                    'product_code': product,
                    'folio': folio,
                    'scheme': scheme,
                    'mongo_units': mongo_units,
                    'dbf_units': dbf_units_rounded,
                    'status': 'MATCHED'
                })
            else:
                mismatched.append({
                    'source': 'CAMS',
                    'product_code': product,
                    'folio': folio,
                    'scheme': scheme,
                    'mongo_units': mongo_units,
                    'dbf_units': dbf_units_rounded,
                    'difference': round(mongo_units - dbf_units_rounded, 3),
                    'status': 'MISMATCHED'
                })
            processed_mongo_keys.add(key)

        else:
            dbf_only.append({
                'source': 'CAMS',
                'product_code': product,
                'folio': folio,
                'scheme': scheme,
                'dbf_units': round(dbf_units, 3),
                'status': 'DBF_ONLY'
            })

        cams_count += 1

    print(f"✓ Processed {cams_count} CAMS records")

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
            'total_processed': cams_count,
            # 'total_processed': cams_count + karvy_count
        }
    }


with DAG(
    dag_id="cams-small-recon-test",
    default_args={
        "owner": "batch_processing",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    agg_map = aggregate_mongodb_transactions()
    reconcile_dbfs(agg_map)
