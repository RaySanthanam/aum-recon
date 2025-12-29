"""Reconciliation logic for matching DBF and MongoDB data."""

from .dbf_reader import read_cams_records, read_karvy_records


def _reconcile(records, aggregations_map, source):
    """
        Core reconciliation logic for comparing DBF records with MongoDB aggregations.
    """
    matched, mismatched, dbf_only = [], [], []
    processed_keys = set()

    for record in records:
        # Extract fields based on source
        if source == "CAMS":
            product = record["PRODUCT"].strip()
            folio = record["FOLIO"].strip()
            scheme = record["SCHEME_NAM"].strip()
            units = round(float(record["UNITS"]), 3)
        else:  # KARVY
            product = record["PRCODE"].strip()
            folio = str(record["ACNO"]).strip()
            scheme = record["FUNDDESC"].strip()
            units = round(float(record["BALUNITS"]), 3)

        key = f"{product}|{folio}|{scheme}"

        if key in aggregations_map:
            mongo_units = aggregations_map[key]
            processed_keys.add(key)

            if mongo_units == units:
                matched.append({
                    "source": source,
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": units,
                    "status": "MATCHED",
                })
            else:
                mismatched.append({
                    "source": source,
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": units,
                    "difference": round(mongo_units - units, 3),
                    "status": "MISMATCHED",
                })
        else:
            dbf_only.append({
                "source": source,
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "dbf_units": units,
                "status": "DBF_ONLY",
            })

    return matched, mismatched, dbf_only, processed_keys


def reconcile_cams(aggregations_map):
    """Reconcile CAMS DBF file against MongoDB aggregations."""
    m, mm, d, p = _reconcile(read_cams_records(), aggregations_map, "CAMS")
    return {
        "matched": m,
        "mismatched": mm,
        "dbf_only": d,
        "processed_keys": list(p),
    }


def reconcile_karvy(aggregations_map):
    """Reconcile KARVY DBF file against MongoDB aggregations."""
    m, mm, d, p = _reconcile(read_karvy_records(), aggregations_map, "KARVY")
    return {
        "matched": m,
        "mismatched": mm,
        "dbf_only": d,
        "processed_keys": list(p),
    }
