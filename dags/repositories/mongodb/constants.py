"""Configuration constants for the AUM reconciliation process."""

# DBF File Paths
CAMS_DBF_PATH = "/opt/airflow/sftp_data/downloads/cams.dbf"
KARVEY_DBF_PATH = "/opt/airflow/sftp_data/downloads/karvey.dbf"

# MongoDB Configuration
MONGO_URI = "mongodb://host.docker.internal:27017"
DB_NAME = "banking_demo"
TRANSACTIONS_COLLECTION = "wealth_pulse_transactions"
RECONCILIATION_COLLECTION = "reconciliation_results"
