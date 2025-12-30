from repositories.mongodb.reconciliation_repo import store_results


def test_store_with_all_record_types(monkeypatch):
    inserted_records = []
    closed = [False]

    def mock_mongo_client(uri):
        class MockCollection:
            def insert_many(self, records):
                inserted_records.extend(records)

        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: MockCollection()
            })(),
            'close': lambda self: closed.__setitem__(0, True)
        })()

    monkeypatch.setattr("repositories.mongodb.reconciliation_repo.MongoClient", mock_mongo_client)

    data = {
        "matched": [{"product_code": "P001"}],
        "mismatched": [{"product_code": "P002"}],
        "dbf_only": [{"product_code": "P003"}],
        "mongo_only": [{"product_code": "P004"}],
    }

    result = store_results(data)

    assert len(inserted_records) == 4
    assert result["total_records"] == 4
    assert closed[0]


def test_store_empty_results(monkeypatch):
    """Test storing empty results."""
    insert_called = [False]
    closed = [False]

    def mock_mongo_client(uri):
        class MockCollection:
            def insert_many(self, records):
                insert_called[0] = True

        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: MockCollection()
            })(),
            'close': lambda self: closed.__setitem__(0, True)
        })()

    monkeypatch.setattr("repositories.mongodb.reconciliation_repo.MongoClient", mock_mongo_client)

    data = {
        "matched": [],
        "mismatched": [],
        "dbf_only": [],
        "mongo_only": [],
    }

    result = store_results(data)

    assert not insert_called[0]
    assert result["total_records"] == 0
    assert closed[0]


def test_records_have_metadata(monkeypatch):
    inserted_records = []

    def mock_mongo_client(uri):
        class MockCollection:
            def insert_many(self, records):
                inserted_records.extend(records)

        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: MockCollection()
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("repositories.mongodb.reconciliation_repo.MongoClient", mock_mongo_client)

    data = {
        "matched": [{"product_code": "P001"}],
        "mismatched": [],
        "dbf_only": [],
        "mongo_only": [],
    }

    store_results(data)

    record = inserted_records[0]
    assert "reconciliation_run_id" in record
    assert "reconciliation_date" in record
    assert "created_at" in record
    assert record["product_code"] == "P001"


def test_mixed_record_counts(monkeypatch):
    """Test storing different counts in each category."""
    inserted_records = []

    def mock_mongo_client(uri):
        class MockCollection:
            def insert_many(self, records):
                inserted_records.extend(records)

        return type('obj', (object,), {
            '__getitem__': lambda self, key: type('obj', (object,), {
                '__getitem__': lambda self, key: MockCollection()
            })(),
            'close': lambda self: None
        })()

    monkeypatch.setattr("repositories.mongodb.reconciliation_repo.MongoClient", mock_mongo_client)

    data = {
        "matched": [{"id": 1}, {"id": 2}],
        "mismatched": [{"id": 3}],
        "dbf_only": [],
        "mongo_only": [{"id": 4}, {"id": 5}, {"id": 6}],
    }

    result = store_results(data)

    assert result["total_records"] == 6
    assert len(inserted_records) == 6
