from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from dbfread import DBF  # type: ignore
from pymongo import MongoClient

CAMS_DBF_PATH = "/opt/airflow/sample_data/20251218140530_12345678901234.dbf"
KARVEY_DBF_PATH = "/opt/airflow/sample_data/W1C1927.dbf"
BATCH_SIZE = 2500


@task
def find_keys_in_dbf(mongo_keys):
    cams_table = DBF(CAMS_DBF_PATH, load=False)

    for record in cams_table:
        product = record['PRODUCT'].strip()
        folio = record['FOLIO'].strip()
        scheme = record['SCHEME_NAM'].strip()

        key = (product, folio, scheme)
        if key in mongo_keys:
            print("match found", key)


@task
def read_mongo_db():
    client = MongoClient("mongodb://host.docker.internal:27017")
    db = client["banking_demo"]
    table = db["wealth_pulse_transactions"]

    transactions = table.find()
    mongo_keys = []

    for record in transactions:
        folio = record["folio_no"].strip()
        scheme = record["scheme_name"].strip()
        product_code = record["scheme_code"].strip()

        key = (product_code, folio, scheme)
        mongo_keys.append(key)

    print(f"Total keys collected: {len(mongo_keys)}", flush=True)
    return mongo_keys


@task
def testing():
    print("started")
    if ('B01', '1014412920', 'HDFC Equity Fund - Growth Option - Regular Plan') == ('B01', '1014412920', 'HDFC Equity Fund - Growth Option - Regular Plan'):
        print("found a match... yayyyyyy")
    else:
        print("no match")
    print("testing task completed")


with DAG(
    dag_id="find_keys",
    default_args={
        "owner": "find_keys",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    mongo_keys = read_mongo_db()
    find_keys_in_dbf(mongo_keys)
