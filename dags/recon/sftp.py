"""SFTP operations for downloading DBF files."""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from utils.sftp_client import SFTPClient  # noqa: E402


def download_dbf_files():
    """Download DBF files from SFTP server."""
    client = SFTPClient(
        host="sftp-server",
        port=22,
        username="testuser",
        password="testpass",
    )

    if client.connect():
        client.download_dbf_files()
        client.disconnect()
