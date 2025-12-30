"""Storage operations for reconciliation results."""

from datetime import datetime
import uuid
from pymongo import MongoClient

from .constants import MONGO_URI, DB_NAME, RECONCILIATION_COLLECTION


def store_results(data):
    """
        Store reconciliation results to MongoDB.
    """
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
