from services.reconciliation_service import (
    reconcile_cams,
    reconcile_karvy,
    merge_reconciliation_results,
)


def test_reconcile_cams_matched(monkeypatch, aggregations_map, cams_record):
    monkeypatch.setattr(
        "services.reconciliation_service.read_cams_records",
        lambda: [cams_record],
    )

    result = reconcile_cams(aggregations_map)

    assert len(result["matched"]) == 1
    assert result["matched"][0]["status"] == "MATCHED"
    assert result["matched"][0]["product_code"] == "P001"
    assert result["mismatched"] == []
    assert result["dbf_only"] == []


def test_reconcile_cams_mismatched(monkeypatch, aggregations_map, cams_record):
    cams_record["UNITS"] = "90"

    monkeypatch.setattr(
        "services.reconciliation_service.read_cams_records",
        lambda: [cams_record],
    )

    result = reconcile_cams(aggregations_map)

    assert len(result["mismatched"]) == 1
    assert result["mismatched"][0]["status"] == "MISMATCHED"
    assert result["mismatched"][0]["difference"] == 10.0
    assert result["matched"] == []


def test_reconcile_cams_dbf_only(monkeypatch, cams_record):
    monkeypatch.setattr(
        "services.reconciliation_service.read_cams_records",
        lambda: [cams_record],
    )

    result = reconcile_cams({})

    assert len(result["dbf_only"]) == 1
    assert result["dbf_only"][0]["status"] == "DBF_ONLY"
    assert result["matched"] == []


def test_reconcile_karvy_matched(monkeypatch, aggregations_map, karvy_record):
    aggregations_map_karvy = {"P001|12345|Scheme A": 100.0}

    monkeypatch.setattr(
        "services.reconciliation_service.read_karvy_records",
        lambda: [karvy_record],
    )

    result = reconcile_karvy(aggregations_map_karvy)

    assert len(result["matched"]) == 1
    assert result["matched"][0]["status"] == "MATCHED"
    assert result["matched"][0]["folio"] == "12345"


def test_reconcile_karvy_mismatched(monkeypatch, karvy_record):
    aggregations_map_karvy = {"P001|12345|Scheme A": 90.0}
    monkeypatch.setattr(
        "services.reconciliation_service.read_karvy_records",
        lambda: [karvy_record],
    )

    result = reconcile_karvy(aggregations_map_karvy)

    assert len(result["mismatched"]) == 1
    assert result["mismatched"][0]["difference"] == -10.0


def test_reconcile_karvy_dbf_only(monkeypatch, karvy_record):
    monkeypatch.setattr(
        "services.reconciliation_service.read_karvy_records",
        lambda: [karvy_record],
    )

    result = reconcile_karvy({})

    assert len(result["dbf_only"]) == 1
    assert result["dbf_only"][0]["status"] == "DBF_ONLY"


def test_reconcile_multiple_records(monkeypatch, aggregations_map):
    records = [
        {
            "PRODUCT": "P001",
            "FOLIO": "F001",
            "SCHEME_NAM": "Scheme A",
            "UNITS": "100.0",
        },
        {
            "PRODUCT": "P002",
            "FOLIO": "F002",
            "SCHEME_NAM": "Scheme B",
            "UNITS": "200.0",
        },
        {
            "PRODUCT": "P003",
            "FOLIO": "F003",
            "SCHEME_NAM": "Scheme C",
            "UNITS": "300.0",
        },
    ]

    aggregations = {
        "P001|F001|Scheme A": 100.0,  # Matched
        "P002|F002|Scheme B": 250.0,  # Mismatched
    }

    monkeypatch.setattr(
        "services.reconciliation_service.read_cams_records",
        lambda: records,
    )

    result = reconcile_cams(aggregations)

    assert len(result["matched"]) == 1
    assert len(result["mismatched"]) == 1
    assert len(result["dbf_only"]) == 1


def test_reconcile_whitespace_handling(monkeypatch, aggregations_map):
    record = {
        "PRODUCT": "  P001  ",
        "FOLIO": "  F001  ",
        "SCHEME_NAM": "  Scheme A  ",
        "UNITS": "100.0",
    }

    monkeypatch.setattr(
        "services.reconciliation_service.read_cams_records",
        lambda: [record],
    )

    result = reconcile_cams(aggregations_map)

    assert len(result["matched"]) == 1
    assert result["matched"][0]["product_code"] == "P001"
    assert result["matched"][0]["folio"] == "F001"
    assert result["matched"][0]["scheme"] == "Scheme A"


def test_merge_reconciliation_results():
    cams_results = {
        "matched": [{"id": 1}],
        "mismatched": [{"id": 2}],
        "dbf_only": [{"id": 3}],
        "processed_keys": ["P001|F001|Scheme A", "P002|F002|Scheme B"],
    }

    karvy_results = {
        "matched": [{"id": 4}],
        "mismatched": [],
        "dbf_only": [{"id": 5}],
        "processed_keys": ["P003|F003|Scheme C"],
    }

    aggregations = {
        "P001|F001|Scheme A": 100.0,
        "P002|F002|Scheme B": 200.0,
        "P003|F003|Scheme C": 300.0,
        "P004|F004|Scheme D": 400.0,  # This should be in mongo_only
    }

    result = merge_reconciliation_results(cams_results, karvy_results, aggregations)

    assert len(result["matched"]) == 2
    assert len(result["mismatched"]) == 1
    assert len(result["dbf_only"]) == 2
    assert len(result["mongo_only"]) == 1
    assert result["mongo_only"][0]["product_code"] == "P004"
    assert result["mongo_only"][0]["folio"] == "F004"
    assert result["mongo_only"][0]["scheme"] == "Scheme D"
    assert result["mongo_only"][0]["mongo_units"] == 400.0
    assert result["mongo_only"][0]["status"] == "MONGO_ONLY"
    assert result["mongo_only"][0]["source"] == "WP_MONGO"
