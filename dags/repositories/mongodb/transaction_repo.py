"""MongoDB transaction aggregation."""

from pymongo import MongoClient

from .constants import MONGO_URI, DB_NAME, TRANSACTIONS_COLLECTION


def aggregate_transactions(transaction_date):
    """
        Aggregate MongoDB transactions by product, folio, and scheme.
    """
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
