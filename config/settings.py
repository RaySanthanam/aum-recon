import os


class Settings:
    """Configuration settings for the reconciliation process."""

    # DBF File Paths
    CAMS_DBF_PATH = os.getenv("CAMS_DBF_PATH", "/opt/airflow/sftp_data/downloads/cams.dbf")
    KARVEY_DBF_PATH = os.getenv("KARVEY_DBF_PATH", "/opt/airflow/sftp_data/downloads/karvey.dbf")

    # MongoDB Configuration
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://host.docker.internal:27017")
    DB_NAME = os.getenv("MONGO_DB_NAME", "banking_demo")
    TRANSACTIONS_COLLECTION = os.getenv("TRANSACTIONS_COLLECTION", "wealth_pulse_transactions")
    RECONCILIATION_COLLECTION = os.getenv("RECONCILIATION_COLLECTION", "reconciliation_results")

    # SFTP Configuration
    SFTP_HOST = os.getenv("SFTP_HOST", "sftp-server")
    SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
    SFTP_USERNAME = os.getenv("SFTP_USERNAME", "testuser")
    SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "testpass")


# Create a singleton instance
settings = Settings()
