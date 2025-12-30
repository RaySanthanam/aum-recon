from clients.sftp_client import SFTPClient


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
