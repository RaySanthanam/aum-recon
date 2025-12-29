"""Tests for MongoDB module."""

from datetime import datetime
from recon.mongo import aggregate_transactions


def test_aggregate_buy_transactions(monkeypatch, mongo_transaction):
    mock_collection = type('obj', (object,), {
        'find': lambda self, query: [mongo_transaction]
    })()

    def mock_mongo_client(uri):
        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: mock_collection
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("recon.mongo.MongoClient", mock_mongo_client)

    result = aggregate_transactions(datetime(2024, 12, 15))

    assert "P001|F001|Scheme A" in result
    assert result["P001|F001|Scheme A"] == 100.0


def test_aggregate_buy_and_sell(monkeypatch):
    transactions = [
        {
            "product_code": "P001",
            "folio_no": "F001",
            "scheme_name": "Scheme A",
            "transaction_type": "BUY",
            "units": 150.0,
        },
        {
            "product_code": "P001",
            "folio_no": "F001",
            "scheme_name": "Scheme A",
            "transaction_type": "SELL",
            "units": 50.0,
        },
    ]

    mock_collection = type('obj', (object,), {
        'find': lambda self, query: transactions
    })()

    def mock_mongo_client(uri):
        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: mock_collection
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("recon.mongo.MongoClient", mock_mongo_client)

    result = aggregate_transactions(datetime(2024, 12, 15))
    assert result["P001|F001|Scheme A"] == 100.0


def test_aggregate_multiple_schemes(monkeypatch):
    transactions = [
        {
            "product_code": "P001",
            "folio_no": "F001",
            "scheme_name": "Scheme A",
            "transaction_type": "BUY",
            "units": 100.0,
        },
        {
            "product_code": "P002",
            "folio_no": "F002",
            "scheme_name": "Scheme B",
            "transaction_type": "BUY",
            "units": 200.0,
        },
    ]

    mock_collection = type('obj', (object,), {
        'find': lambda self, query: transactions
    })()

    def mock_mongo_client(uri):
        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: mock_collection
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("recon.mongo.MongoClient", mock_mongo_client)

    result = aggregate_transactions(datetime(2024, 12, 15))

    assert len(result) == 2
    assert result["P001|F001|Scheme A"] == 100.0
    assert result["P002|F002|Scheme B"] == 200.0


def test_aggregate_rounding(monkeypatch):
    transaction = {
        "product_code": "P001",
        "folio_no": "F001",
        "scheme_name": "Scheme A",
        "transaction_type": "BUY",
        "units": 100.123456,
    }

    mock_collection = type('obj', (object,), {
        'find': lambda self, query: [transaction]
    })()

    def mock_mongo_client(uri):
        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: mock_collection
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("recon.mongo.MongoClient", mock_mongo_client)

    result = aggregate_transactions(datetime(2024, 12, 15))
    assert result["P001|F001|Scheme A"] == 100.123


def test_aggregate_empty_result(monkeypatch):
    mock_collection = type('obj', (object,), {
        'find': lambda self, query: []
    })()

    def mock_mongo_client(uri):
        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: mock_collection
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("recon.mongo.MongoClient", mock_mongo_client)

    result = aggregate_transactions(datetime(2024, 12, 15))
    assert result == {}
