from dbfread import DBF  # type: ignore
from pymongo import MongoClient

CAMS_DBF_PATH = "/Users/ray/Desktop/airflow/brokerage-recon/sample_data/20251218140530_12345678901234.dbf"


mongo_keys = []


def find_keys_in_dbf():
    cams_table = DBF(CAMS_DBF_PATH, load=False)

    for record in cams_table:
        product = record['PRODUCT'].strip()
        folio = record['FOLIO'].strip()
        scheme = record['SCHEME_NAM'].strip()

        key = (product, folio, scheme)
        if key in mongo_keys:
            print("match found", key)


def read_mongo_db():
    client = MongoClient("mongodb://localhost:27017")
    db = client["banking_demo"]
    table = db["wealth_pulse_transactions"]

    transactions = table.find()
    print(transactions)

    for record in table.find():
        folio = record["folio_no"].strip()
        scheme = record["scheme_name"].strip()
        product_code = record["scheme_code"].strip()

        key = (product_code, folio, scheme)
        mongo_keys.append(key)
        print(key)


read_mongo_db()
find_keys_in_dbf()
