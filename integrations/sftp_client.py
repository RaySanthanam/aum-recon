import paramiko
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SFTPClient:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = None
        self.sftp_client = None

    def connect(self):
        try:
            self.ssh_client = paramiko.SSHClient()
            # Security: Only accept known hosts (not AutoAddPolicy which accepts any host)
            self.ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy())

            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=10
            )
            self.sftp_client = self.ssh_client.open_sftp()

            logger.info(f"Connected to SFTP server {self.host}:{self.port}")
            return True

        except Exception as e:
            logger.error(f"SFTP connection failed: {e}")
            return False

    def disconnect(self):
        if self.sftp_client:
            self.sftp_client.close()

        if self.ssh_client:
            self.ssh_client.close()

    def list_files(self, remote_dir="/uploads"):
        try:
            files = self.sftp_client.listdir(remote_dir)
            logger.info(f"Found {len(files)} files in {remote_dir}")

            for f in files:
                logger.debug(f"  - {f}")
            return files

        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []

    def download_dbf_files(self, remote_dir="/uploads", local_dir="/opt/airflow/sftp_data/downloads"):
        try:
            files = self.list_files(remote_dir)

            dbf_files = [f for f in files if f.lower().endswith('.dbf')]
            logger.info(f"Found {len(dbf_files)} DBF files to download")
            downloaded = []
            for filename in dbf_files:
                remote_path = f"{remote_dir}/{filename}"
                local_path = f"{local_dir}/{filename}"

                try:
                    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                    self.sftp_client.get(remote_path, local_path)
                    file_size = Path(local_path).stat().st_size
                    logger.info(f"Downloaded {filename} ({file_size} bytes)")
                    downloaded.append(filename)

                except Exception as e:
                    logger.error(f"Failed to download {filename}: {e}")

            logger.info(f"Downloaded {len(downloaded)}/{len(dbf_files)} files")
            return downloaded

        except Exception as e:
            logger.error(f"Bulk download failed: {e}")
            return []
