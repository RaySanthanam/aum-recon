from datetime import datetime
from dbfread import DBF  # type: ignore
from typing import Optional


class DBFProcessor:
    def __init__(self, cams_path: str, karvy_path: str):
        self.cams_path = cams_path
        self.karvy_path = karvy_path

    def extract_date(self) -> datetime:
        # Try CAMS first
        date = self._extract_date_from_cams()
        if date:
            return date

        # Fallback to KARVY
        date = self._extract_date_from_karvy()
        if date:
            return date

        raise RuntimeError("Failed to extract date from DBF files")

    def _extract_date_from_cams(self) -> Optional[datetime]:
        # Extract date from CAMS DBF file.
        try:
            cams_table = DBF(self.cams_path, load=False)
            for record in cams_table:
                asset_date = record.get("ASSET_DATE", "").strip()
                if asset_date:
                    return datetime.strptime(asset_date, "%d-%m-%Y")
        except Exception:
            pass
        return None

    def _extract_date_from_karvy(self) -> Optional[datetime]:
        # Extract date from KARVY DBF file.
        try:
            karvy_table = DBF(self.karvy_path, load=False)
            for record in karvy_table:
                trdate = record.get("TRDATE", "").strip()
                if trdate:
                    return datetime.strptime(trdate, "%d-%m-%Y")
        except Exception:
            pass
        return None

    def read_cams_records(self):
        cams_table = DBF(self.cams_path, load=False)
        for record in cams_table:
            yield {
                "product_code": record["PRODUCT"].strip(),
                "folio": record["FOLIO"].strip(),
                "scheme": record["SCHEME_NAM"].strip(),
                "units": round(float(record["UNITS"]), 3),
                "source": "CAMS"
            }

    def read_karvy_records(self):
        karvy_table = DBF(self.karvy_path, load=False)
        for record in karvy_table:
            yield {
                "product_code": record["PRCODE"].strip(),
                "folio": str(record["ACNO"]).strip(),
                "scheme": record["FUNDDESC"].strip(),
                "units": round(float(record["BALUNITS"]), 3),
                "source": "KARVY"
            }
