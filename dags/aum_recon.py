"""AUM Reconciliation DAG - Parallel processing of CAMS and KARVY data."""

from airflow import DAG
from airflow.sdk.definitions.decorators import task
from datetime import datetime, timedelta

from services.sftp_service import download_dbf_files
from services.dbf_reader import extract_transaction_date
from repositories.mongodb.transaction_repo import aggregate_transactions
from services.reconciliation_service import (
    reconcile_cams,
    reconcile_karvy,
    merge_reconciliation_results,
)
from repositories.mongodb.reconciliation_repo import store_results


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
def reconcile_cams_task(aggregations):
    """Reconcile CAMS DBF file against MongoDB aggregations."""
    return reconcile_cams(aggregations)


@task
def reconcile_karvy_task(aggregations):
    """Reconcile KARVY DBF file against MongoDB aggregations."""
    return reconcile_karvy(aggregations)


@task
def merge_results(cams, karvy, aggregations):
    """Merge CAMS and KARVY results and identify MongoDB-only records."""
    return merge_reconciliation_results(cams, karvy, aggregations)


@task
def store_in_db(data):
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

    download_files = download()
    transaction_date = extract_date()
    aggregated_transactions = aggregate(transaction_date)

    cams_results = reconcile_cams_task(aggregated_transactions)
    karvy_results = reconcile_karvy_task(aggregated_transactions)

    merged_results = merge_results(cams_results, karvy_results, aggregated_transactions)
    stored_results = store_in_db(merged_results)

    download_files >> transaction_date >> aggregated_transactions
    aggregated_transactions >> [cams_results, karvy_results]
    [cams_results, karvy_results] >> merged_results >> stored_results
