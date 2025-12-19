"""
Generate test data for CAMS, Karvy DBF files and MongoDB transactions.

This script creates:
- MongoDB: 50 transactions that aggregate to 20 unique records
- Karvy DBF: 10 records (7 matched, 2 mismatched, 1 DBF-only)
- CAMS DBF: 8 records (6 matched, 1 mismatched, 1 DBF-only)

Final reconciliation results:
- Matched: 13 (7 Karvy + 6 CAMS)
- Mismatched: 3 (2 Karvy + 1 CAMS)
- MongoDB only: 2
- DBF only: 2 (1 Karvy + 1 CAMS)

Installation:
    pip install dbf pymongo

Usage:
    python generate_test_data.py
"""

import dbf
from datetime import datetime, timedelta
import random
from pymongo import MongoClient
import os


# Test data structure
# Each entry has: (product_code, folio, scheme_name, expected_units)

MATCHED_RECORDS = [
    # Karvy matched (7 records)
    ("K01", "19951572335", "HDFC Equity Fund - Growth", 1500.250),
    ("K02", "19951572336", "ICICI Prudential Balanced Fund", 2000.500),
    ("K03", "19951572337", "SBI Blue Chip Fund - Regular", 750.125),
    ("K04", "19951572338", "Axis Long Term Equity Fund", 1200.750),
    ("K05", "19951572339", "Kotak Tax Saver Fund - Growth", 950.500),
    ("K06", "19951572340", "DSP Tax Saver Fund - Regular", 1800.250),
    ("K07", "19951572341", "UTI Equity Fund - Growth Plan", 2500.125),

    # CAMS matched (6 records)
    ("B01", "1014433353", "Aditya Birla Sun Life ELSS Tax Saver Fund", 916.253),
    ("B02", "1014433354", "Birla Sun Life Frontline Equity Fund", 1250.500),
    ("B03", "1014433355", "Aditya Birla Sun Life Pure Value Fund", 800.125),
    ("B04", "1014433356", "Birla Sun Life Tax Relief 96", 1500.750),
    ("B05", "1014433357", "Aditya Birla Sun Life Advantage Fund", 2200.250),
    ("B06", "1014433358", "Birla Sun Life Dividend Yield Fund", 1100.125),
]

MISMATCHED_RECORDS = [
    # Karvy mismatched (2 records) - DBF units will differ
    # Format: (product, folio, scheme, mongo_units, dbf_units)
    ("K08", "19951572342", "Franklin India Equity Fund", 1000.500, 1050.750),
    ("K09", "19951572343", "Reliance Growth Fund - Regular", 1500.250, 1450.125),

    # CAMS mismatched (1 record)
    ("B07", "1014433359", "Birla Sun Life Mid Cap Fund", 1800.125, 1850.500),
]

MONGO_ONLY_RECORDS = [
    # Only in MongoDB (2 records)
    ("M01", "9999999991", "Tata Equity PE Fund - Growth", 500.250),
    ("M02", "9999999992", "Mirae Asset Large Cap Fund", 750.125),
]

DBF_ONLY_RECORDS = [
    # Karvy only (1 record)
    ("K10", "19951572344", "Sundaram Select Mid Cap Fund", 650.500, "KARVY"),

    # CAMS only (1 record)
    ("B08", "1014433360", "Birla Sun Life Cash Plus Fund", 900.250, "CAMS"),
]


def generate_mongo_transactions():
    """Generate 50 MongoDB transactions that aggregate to 20 unique records."""

    transactions = []

    # Helper function to create BUY/SELL transactions that sum to target
    def create_transactions_for_record(product, folio, scheme, target_units):
        txns = []

        # Strategy: Create 2-3 BUY transactions and 0-1 SELL transactions
        num_buys = random.randint(2, 3)
        has_sell = random.choice([True, False])

        # Generate BUY amounts that sum more than target
        buy_amounts = []
        remaining = target_units

        if has_sell:
            # If we have a SELL, BUYs should sum to more than target
            total_buy = target_units + random.uniform(50, 200)
            for i in range(num_buys - 1):
                amount = random.uniform(total_buy * 0.2, total_buy * 0.4)
                buy_amounts.append(round(amount, 3))
            buy_amounts.append(round(total_buy - sum(buy_amounts), 3))

            # SELL amount is the difference
            sell_amount = round(total_buy - target_units, 3)
        else:
            # No SELL, BUYs sum exactly to target
            for i in range(num_buys - 1):
                amount = random.uniform(remaining * 0.2, remaining * 0.5)
                buy_amounts.append(round(amount, 3))
                remaining -= amount
            buy_amounts.append(round(remaining, 3))

        # Create BUY transactions
        for amount in buy_amounts:
            txns.append({
                "folio_no": folio,
                "scheme_name": scheme,
                "scheme_code": product,
                "units": amount,
                "transaction_type": "BUY",
                "transaction_date": datetime.now() - timedelta(days=random.randint(1, 365)),
                "nav": round(random.uniform(10, 500), 2),
                "amount": round(amount * random.uniform(10, 500), 2)
            })

        # Create SELL transaction if needed
        if has_sell:
            txns.append({
                "folio_no": folio,
                "scheme_name": scheme,
                "scheme_code": product,
                "units": sell_amount,
                "transaction_type": "SELL",
                "transaction_date": datetime.now() - timedelta(days=random.randint(1, 180)),
                "nav": round(random.uniform(10, 500), 2),
                "amount": round(sell_amount * random.uniform(10, 500), 2)
            })

        return txns

    # Generate transactions for matched records
    for product, folio, scheme, units in MATCHED_RECORDS:
        transactions.extend(create_transactions_for_record(product, folio, scheme, units))

    # Generate transactions for mismatched records (use mongo units)
    for product, folio, scheme, mongo_units, dbf_units in MISMATCHED_RECORDS:
        transactions.extend(create_transactions_for_record(product, folio, scheme, mongo_units))

    # Generate transactions for mongo-only records
    for product, folio, scheme, units in MONGO_ONLY_RECORDS:
        transactions.extend(create_transactions_for_record(product, folio, scheme, units))

    print(f"Generated {len(transactions)} MongoDB transactions")
    print(f"These aggregate to {len(MATCHED_RECORDS) + len(MISMATCHED_RECORDS) + len(MONGO_ONLY_RECORDS)} unique records")

    return transactions


def generate_karvy_dbf(filename="sftp_data/uploads/karvey.dbf"):
    """Generate Karvy DBF file with 10 records."""

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Create DBF with Karvy structure
    table = dbf.Table(
        filename,
        'PRCODE C(10); FUND C(5); ACNO N(15,0); SCHEME C(3); DIVOPT C(1); '
        'FUNDDESC C(254); BALUNITS N(20,3); PLDG N(20,3); TRDATE C(10); '
        'TRDESC C(40); MOH C(2); BROKCODE C(30); SBCODE C(20); POUT C(1); '
        'INV_ID C(1); INVNAME C(160); ADD1 C(40); ADD2 C(40); ADD3 C(40); '
        'CITY C(40); INV_PIN N(10,0); RPHONE C(40); OPHONE C(40); FAX C(40); '
        'EMAIL C(80); VALINV N(20,2); LNAV C(30); CRDATE C(10); CRTIME C(6); TODATE C(10)'
    )
    table.open(mode=dbf.READ_WRITE)

    records_added = 0

    # Add matched records from Karvy (7 records)
    for product, folio, scheme, units in MATCHED_RECORDS[:7]:
        table.append({
            'PRCODE': product,
            'FUND': '101',
            'ACNO': int(folio),
            'SCHEME': 'EQT',
            'DIVOPT': 'G',
            'FUNDDESC': scheme,
            'BALUNITS': units,
            'PLDG': 0.0,
            'TRDATE': datetime.now().strftime("%d-%m-%Y"),
            'TRDESC': 'Balance',
            'MOH': '',
            'BROKCODE': 'ARN-110136',
            'SBCODE': '',
            'POUT': 'N',
            'INV_ID': 'I',
            'INVNAME': 'Test Investor ' + folio[-4:],
            'ADD1': 'Address Line 1',
            'ADD2': 'Address Line 2',
            'ADD3': '',
            'CITY': 'Mumbai',
            'INV_PIN': 400001,
            'RPHONE': '',
            'OPHONE': '',
            'FAX': '',
            'EMAIL': f'investor{folio[-4:]}@example.com',
            'VALINV': round(units * random.uniform(100, 300), 2),
            'LNAV': str(round(random.uniform(100, 300), 2)),
            'CRDATE': datetime.now().strftime("%d-%m-%Y"),
            'CRTIME': datetime.now().strftime("%H%M%S"),
            'TODATE': datetime.now().strftime("%d-%m-%Y")
        })
        records_added += 1

    # Add mismatched records from Karvy (2 records) - use DBF units
    for product, folio, scheme, mongo_units, dbf_units in MISMATCHED_RECORDS[:2]:
        table.append({
            'PRCODE': product,
            'FUND': '101',
            'ACNO': int(folio),
            'SCHEME': 'EQT',
            'DIVOPT': 'G',
            'FUNDDESC': scheme,
            'BALUNITS': dbf_units,  # Different from MongoDB
            'PLDG': 0.0,
            'TRDATE': datetime.now().strftime("%d-%m-%Y"),
            'TRDESC': 'Balance',
            'MOH': '',
            'BROKCODE': 'ARN-110136',
            'SBCODE': '',
            'POUT': 'N',
            'INV_ID': 'I',
            'INVNAME': 'Test Investor ' + folio[-4:],
            'ADD1': 'Address Line 1',
            'ADD2': 'Address Line 2',
            'ADD3': '',
            'CITY': 'Delhi',
            'INV_PIN': 110001,
            'RPHONE': '',
            'OPHONE': '',
            'FAX': '',
            'EMAIL': f'investor{folio[-4:]}@example.com',
            'VALINV': round(dbf_units * random.uniform(100, 300), 2),
            'LNAV': str(round(random.uniform(100, 300), 2)),
            'CRDATE': datetime.now().strftime("%d-%m-%Y"),
            'CRTIME': datetime.now().strftime("%H%M%S"),
            'TODATE': datetime.now().strftime("%d-%m-%Y")
        })
        records_added += 1

    # Add Karvy-only record (1 record)
    for product, folio, scheme, units, source in [r for r in DBF_ONLY_RECORDS if r[4] == "KARVY"]:
        table.append({
            'PRCODE': product,
            'FUND': '101',
            'ACNO': int(folio),
            'SCHEME': 'EQT',
            'DIVOPT': 'G',
            'FUNDDESC': scheme,
            'BALUNITS': units,
            'PLDG': 0.0,
            'TRDATE': datetime.now().strftime("%d-%m-%Y"),
            'TRDESC': 'Balance',
            'MOH': '',
            'BROKCODE': 'ARN-110136',
            'SBCODE': '',
            'POUT': 'N',
            'INV_ID': 'I',
            'INVNAME': 'Test Investor ' + folio[-4:],
            'ADD1': 'Address Line 1',
            'ADD2': 'Address Line 2',
            'ADD3': '',
            'CITY': 'Bangalore',
            'INV_PIN': 560001,
            'RPHONE': '',
            'OPHONE': '',
            'FAX': '',
            'EMAIL': f'investor{folio[-4:]}@example.com',
            'VALINV': round(units * random.uniform(100, 300), 2),
            'LNAV': str(round(random.uniform(100, 300), 2)),
            'CRDATE': datetime.now().strftime("%d-%m-%Y"),
            'CRTIME': datetime.now().strftime("%H%M%S"),
            'TODATE': datetime.now().strftime("%d-%m-%Y")
        })
        records_added += 1

    table.close()
    print(f"Generated Karvy DBF with {records_added} records")
    print(f"  - Matched: 7")
    print(f"  - Mismatched: 2")
    print(f"  - Karvy only: 1")


def generate_cams_dbf(filename="sftp_data/uploads/cams.dbf"):
    """Generate CAMS DBF file with 8 records."""

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Create DBF with CAMS structure
    table = dbf.Table(
        filename,
        'BROK_DLR_C C(20); PRODUCT C(10); ASSET_DATE C(20); FOLIO C(20); '
        'INV_NAME C(100); SCHEME_NAM C(200); CLOSING_AS N(20,3); CITY C(50); '
        'AE_CODE C(20); TAX_STATUS C(50); UNITS N(20,3); NAV N(20,3); '
        'INV_IIN N(10,0); FOLIO_OLD C(20); SCHEME_FOL C(20)'
    )
    table.open(mode=dbf.READ_WRITE)

    records_added = 0

    # Add matched records from CAMS (6 records)
    for product, folio, scheme, units in MATCHED_RECORDS[7:13]:
        nav = round(random.uniform(100, 300), 3)
        table.append({
            'BROK_DLR_C': 'ARN-110136',
            'PRODUCT': product,
            'ASSET_DATE': datetime.now().strftime("%d-%m-%Y"),
            'FOLIO': folio,
            'INV_NAME': 'Test Investor ' + folio[-4:],
            'SCHEME_NAM': scheme,
            'CLOSING_AS': round(units * nav, 3),
            'CITY': random.choice(["Mumbai", "Delhi", "Chennai", "Bangalore", "Hyderabad"]),
            'AE_CODE': '',
            'TAX_STATUS': 'Individual',
            'UNITS': units,
            'NAV': nav,
            'INV_IIN': 0,
            'FOLIO_OLD': '',
            'SCHEME_FOL': ''
        })
        records_added += 1

    # Add mismatched record from CAMS (1 record) - use DBF units
    for product, folio, scheme, mongo_units, dbf_units in MISMATCHED_RECORDS[2:3]:
        nav = round(random.uniform(100, 300), 3)
        table.append({
            'BROK_DLR_C': 'ARN-110136',
            'PRODUCT': product,
            'ASSET_DATE': datetime.now().strftime("%d-%m-%Y"),
            'FOLIO': folio,
            'INV_NAME': 'Test Investor ' + folio[-4:],
            'SCHEME_NAM': scheme,
            'CLOSING_AS': round(dbf_units * nav, 3),  # Different from MongoDB
            'CITY': 'Pune',
            'AE_CODE': '',
            'TAX_STATUS': 'Individual',
            'UNITS': dbf_units,  # Different from MongoDB
            'NAV': nav,
            'INV_IIN': 0,
            'FOLIO_OLD': '8002440',
            'SCHEME_FOL': ''
        })
        records_added += 1

    # Add CAMS-only record (1 record)
    for product, folio, scheme, units, source in [r for r in DBF_ONLY_RECORDS if r[4] == "CAMS"]:
        nav = round(random.uniform(100, 300), 3)
        table.append({
            'BROK_DLR_C': 'ARN-110136',
            'PRODUCT': product,
            'ASSET_DATE': datetime.now().strftime("%d-%m-%Y"),
            'FOLIO': folio,
            'INV_NAME': 'Test Investor ' + folio[-4:],
            'SCHEME_NAM': scheme,
            'CLOSING_AS': round(units * nav, 3),
            'CITY': 'Kolkata',
            'AE_CODE': '',
            'TAX_STATUS': 'HUF',
            'UNITS': units,
            'NAV': nav,
            'INV_IIN': 0,
            'FOLIO_OLD': '',
            'SCHEME_FOL': ''
        })
        records_added += 1

    table.close()
    print(f"Generated CAMS DBF with {records_added} records")
    print(f"  - Matched: 6")
    print(f"  - Mismatched: 1")
    print(f"  - CAMS only: 1")


def insert_mongo_transactions(transactions, db_name="banking_demo", collection_name="wealth_pulse_transactions"):
    """Insert transactions into MongoDB."""

    # Try localhost first, then host.docker.internal
    mongo_urls = [
        "mongodb://localhost:27017",
        "mongodb://host.docker.internal:27017"
    ]

    client = None
    for url in mongo_urls:
        try:
            client = MongoClient(url, serverSelectionTimeoutMS=5000)
            # Test connection
            client.admin.command('ping')
            print(f"Connected to MongoDB at {url}")
            break
        except Exception as e:
            print(f"Could not connect to {url}: {e}")
            continue

    if client is None:
        raise Exception("Could not connect to MongoDB on any available host")

    db = client[db_name]
    collection = db[collection_name]

    # Clear existing data
    collection.delete_many({})

    # Insert new transactions
    collection.insert_many(transactions)

    print(f"Inserted {len(transactions)} transactions into MongoDB")
    print(f"Database: {db_name}, Collection: {collection_name}")

    client.close()


def main():
    """Main function to generate all test data."""

    print("=" * 70)
    print("GENERATING TEST DATA FOR BROKERAGE RECONCILIATION")
    print("=" * 70)
    print()

    # Generate MongoDB transactions
    print("1. Generating MongoDB transactions...")
    transactions = generate_mongo_transactions()
    print()

    # Generate Karvy DBF
    print("2. Generating Karvy DBF file...")
    generate_karvy_dbf()
    print()

    # Generate CAMS DBF
    print("3. Generating CAMS DBF file...")
    generate_cams_dbf()
    print()

    # Insert into MongoDB
    print("4. Inserting transactions into MongoDB...")
    insert_mongo_transactions(transactions)
    print()

    print("=" * 70)
    print("EXPECTED RECONCILIATION RESULTS:")
    print("=" * 70)
    print(f"Total MongoDB records (after aggregation): 20")
    print(f"Total Karvy DBF records: 10")
    print(f"Total CAMS DBF records: 8")
    print()
    print(f"Matched (units match): 13 (7 Karvy + 6 CAMS)")
    print(f"Mismatched (units differ): 3 (2 Karvy + 1 CAMS)")
    print(f"MongoDB only: 2")
    print(f"DBF only: 2 (1 Karvy + 1 CAMS)")
    print("=" * 70)


if __name__ == "__main__":
    main()
