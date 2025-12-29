from typing import Dict, Callable


def reconcile_source(
    record_generator: Callable,
    aggregations_map: Dict[str, float],
    source_name: str
) -> Dict:
    matched, mismatched, dbf_only = [], [], []
    processed_keys = set()

    for record in record_generator():
        product = record["product_code"]
        folio = record["folio"]
        scheme = record["scheme"]
        dbf_units = record["units"]

        key = f"{product}|{folio}|{scheme}"

        if key in aggregations_map:
            mongo_units = aggregations_map[key]
            processed_keys.add(key)

            if mongo_units == dbf_units:
                matched.append({
                    "source": source_name,
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "status": "MATCHED",
                })
            else:
                mismatched.append({
                    "source": source_name,
                    "product_code": product,
                    "folio": folio,
                    "scheme": scheme,
                    "mongo_units": mongo_units,
                    "dbf_units": dbf_units,
                    "difference": round(mongo_units - dbf_units, 3),
                    "status": "MISMATCHED",
                })
        else:
            dbf_only.append({
                "source": source_name,
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "dbf_units": dbf_units,
                "status": "DBF_ONLY",
            })

    return {
        "matched": matched,
        "mismatched": mismatched,
        "dbf_only": dbf_only,
        "processed_keys": list(processed_keys),
    }


def merge_results(
    cams_results: Dict,
    karvy_results: Dict,
    aggregations_map: Dict[str, float]
) -> Dict:
    processed = set(cams_results["processed_keys"] + karvy_results["processed_keys"])

    mongo_only = []
    for key, units in aggregations_map.items():
        if key not in processed:
            product, folio, scheme = key.split("|")
            mongo_only.append({
                "source": "WP_MONGO",
                "product_code": product,
                "folio": folio,
                "scheme": scheme,
                "mongo_units": units,
                "status": "MONGO_ONLY",
            })

    return {
        "matched": cams_results["matched"] + karvy_results["matched"],
        "mismatched": cams_results["mismatched"] + karvy_results["mismatched"],
        "dbf_only": cams_results["dbf_only"] + karvy_results["dbf_only"],
        "mongo_only": mongo_only,
    }
