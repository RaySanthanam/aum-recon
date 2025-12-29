from datetime import datetime
from pymongo import MongoClient
from typing import Dict, List
from contextlib import contextmanager
import uuid
import logging

logger = logging.getLogger(__name__)


@contextmanager
def get_mongo_connection(mongo_uri: str):
    client = None
    try:
        client = MongoClient(mongo_uri)
        yield client
    finally:
        if client:
            client.close()


def aggregate_transactions(
    mongo_uri: str,
    db_name: str,
    transactions_collection: str,
    transaction_date: datetime
) -> Dict[str, float]:
    with get_mongo_connection(mongo_uri) as client:
        collection = client[db_name][transactions_collection]

        aggregations: Dict[str, float] = {}
        query = {"transaction_date": {"$lte": transaction_date}}

        for txn in collection.find(query):
            key = f"{txn['product_code']}|{txn['folio_no']}|{txn['scheme_name']}"
            aggregations.setdefault(key, 0.0)

            if txn["transaction_type"] == "BUY":
                aggregations[key] += txn["units"]
            elif txn["transaction_type"] == "SELL":
                aggregations[key] -= txn["units"]

        return {k: round(v, 3) for k, v in aggregations.items()}


def store_reconciliation_results(
    mongo_uri: str,
    db_name: str,
    reconciliation_collection: str,
    data: Dict[str, List[Dict]]
) -> Dict:
    with get_mongo_connection(mongo_uri) as client:
        collection = client[db_name][reconciliation_collection]

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

        # Log summary
        _log_summary(run_id, data, len(records), db_name, reconciliation_collection)

        return {"run_id": run_id, "total_records": len(records)}


def _log_summary(
    run_id: str,
    data: Dict[str, List],
    total_records: int,
    db_name: str,
    collection_name: str
) -> None:
    logger.info("=" * 70)
    logger.info("RECONCILIATION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Run ID:                      {run_id}")
    logger.info(f"Total Records Inserted:      {total_records}")
    logger.info(f"  - Matched:                 {len(data['matched'])}")
    logger.info(f"  - Mismatched:              {len(data['mismatched'])}")
    logger.info(f"  - Available Only in DBF:   {len(data['dbf_only'])}")
    logger.info(f"  - Available Only in Mongo: {len(data['mongo_only'])}")
    logger.info(f"Collection:                  {db_name}.{collection_name}")
    logger.info("=" * 70)
