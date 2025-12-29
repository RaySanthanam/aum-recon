"""Tests for DBF reader module."""

import pytest
from datetime import datetime
from recon.dbf_reader import extract_transaction_date, read_cams_records, read_karvy_records


def test_extract_from_cams_success(monkeypatch):
    mock_record = {"ASSET_DATE": "15-12-2024"}

    def mock_dbf(*args, **kwargs):
        return iter([mock_record])

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    result = extract_transaction_date()
    assert result == datetime(2024, 12, 15)


def test_extract_from_karvy_fallback(monkeypatch):
    mock_karvy_record = {"TRDATE": "20-11-2024"}
    call_count = [0]

    def mock_dbf(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("CAMS file not found")
        return iter([mock_karvy_record])

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    result = extract_transaction_date()
    assert result == datetime(2024, 11, 20)


def test_extract_with_whitespace(monkeypatch):
    mock_record = {"ASSET_DATE": "  15-12-2024  "}

    def mock_dbf(*args, **kwargs):
        return iter([mock_record])

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    result = extract_transaction_date()
    assert result == datetime(2024, 12, 15)


def test_read_cams_returns_dbf(monkeypatch):
    mock_dbf_obj = object()

    def mock_dbf(path, load):
        assert path == "/opt/airflow/sftp_data/downloads/cams.dbf"
        assert load is False
        return mock_dbf_obj

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    result = read_cams_records()
    assert result == mock_dbf_obj


def test_read_karvy_returns_dbf(monkeypatch):
    mock_dbf_obj = object()

    def mock_dbf(path, load):
        assert path == "/opt/airflow/sftp_data/downloads/karvey.dbf"
        assert load is False
        return mock_dbf_obj

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    result = read_karvy_records()
    assert result == mock_dbf_obj


def test_extract_both_files_fail(monkeypatch):
    """Test exception when both CAMS and KARVY fail."""
    def mock_dbf(*args, **kwargs):
        raise Exception("File not found")

    monkeypatch.setattr("recon.dbf_reader.DBF", mock_dbf)

    with pytest.raises(RuntimeError, match="Failed to extract date from DBF files"):
        extract_transaction_date()
