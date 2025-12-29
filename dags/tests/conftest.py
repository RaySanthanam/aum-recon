"""Shared test fixtures."""

import pytest


@pytest.fixture
def cams_record():
    """Sample CAMS DBF record."""
    return {
        "PRODUCT": "P001",
        "FOLIO": "F001",
        "SCHEME_NAM": "Scheme A",
        "UNITS": "100.0",
    }


@pytest.fixture
def karvy_record():
    """Sample KARVY DBF record."""
    return {
        "PRCODE": "P001",
        "ACNO": 12345,
        "FUNDDESC": "Scheme A",
        "BALUNITS": "100.0",
    }


@pytest.fixture
def aggregations_map():
    """Sample MongoDB aggregations."""
    return {"P001|F001|Scheme A": 100.0}


@pytest.fixture
def mongo_transaction():
    """Sample MongoDB transaction."""
    return {
        "product_code": "P001",
        "folio_no": "F001",
        "scheme_name": "Scheme A",
        "transaction_type": "BUY",
        "units": 100.0,
    }
