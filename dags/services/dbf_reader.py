from datetime import datetime
from dbfread import DBF  # type: ignore

from repositories.mongodb.constants import CAMS_DBF_PATH, KARVEY_DBF_PATH


def extract_transaction_date():
    """Extract transaction date from DBF files. Tries CAMS first, then KARVY."""
    # Try CAMS
    try:
        for record in DBF(CAMS_DBF_PATH, load=False):
            asset_date = record.get("ASSET_DATE", "").strip()
            return datetime.strptime(asset_date, "%d-%m-%Y")
    except Exception:
        pass

    # Fallback to KARVY
    try:
        for record in DBF(KARVEY_DBF_PATH, load=False):
            trdate = record.get("TRDATE", "").strip()
            return datetime.strptime(trdate, "%d-%m-%Y")
    except Exception as exc:
        raise RuntimeError("Failed to extract date from DBF files") from exc


def read_cams_records():
    """Read CAMS DBF records using lazy loading."""
    return DBF(CAMS_DBF_PATH, load=False)


def read_karvy_records():
    """Read KARVY DBF records using lazy loading."""
    return DBF(KARVEY_DBF_PATH, load=False)
