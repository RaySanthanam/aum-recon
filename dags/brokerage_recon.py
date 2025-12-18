from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta
from dbfread import DBF  # type: ignore
from pymongo import MongoClient

DBF_PATH = "/opt/airflow/sample_data/transactions.dbf"
BATCH_SIZE = 2500


def read_dbf_in_batches(dbf_path: str, batch_size: int):
    batch = []
    table = DBF(dbf_path, load=False)
    for record in table:
        batch.append(dict(record))

        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def insert_batch_into_mongo(batch):
    client = MongoClient("mongodb://host.docker.internal:27017/")
    db = client["banking_demo"]
    collection = db["aum_recon"]

    collection.insert_many(batch)

    client.close()


default_args = {
    "owner": "batch_processing",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}


@task
def process_dbf_file():
    batch_no = 0

    for batch in read_dbf_in_batches(DBF_PATH, BATCH_SIZE):
        insert_batch_into_mongo(batch)
        batch_no += 1
        print(f"✓ Inserted batch {batch_no}")


with DAG(
    dag_id="batch_processing",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "safe"],
) as dag:

    process_dbf_file()
