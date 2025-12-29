"""AUM Reconciliation DAG - Parallel processing of CAMS and KARVY data."""

from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta

from recon.sftp import download_dbf_files
from recon.dbf_reader import extract_transaction_date
from recon.mongo import aggregate_transactions
from recon.reconcile import reconcile_cams, reconcile_karvy
from recon.store import store_results


@task
def download():
    """Download DBF files from SFTP server."""
    download_dbf_files()


@task
def extract_date():
    """Extract transaction date from DBF files."""
    return extract_transaction_date()


@task
def aggregate(txn_date):
    """Aggregate MongoDB transactions up to the given date."""
    return aggregate_transactions(txn_date)


@task
def merge(cams, karvy, aggregations):
    """Merge CAMS and KARVY results and identify MongoDB-only records."""
    processed = set(cams["processed_keys"] + karvy["processed_keys"])

    mongo_only = []
    for key, units in aggregations.items():
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
def store(data):
    """Store reconciliation results to MongoDB."""
    return store_results(data)


with DAG(
    dag_id="aum_recon",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "batch_processing",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
) as dag:

    d = download()
    dt = extract_date()
    agg = aggregate(dt)

    cams = reconcile_cams(agg)
    karvy = reconcile_karvy(agg)

    merged = merge(cams, karvy, agg)
    store_task = store(merged)

    d >> dt >> agg
    agg >> [cams, karvy]
    [cams, karvy] >> merged >> store_task
