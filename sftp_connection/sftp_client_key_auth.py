#!/usr/bin/env python3
"""
SFTP Client with SSH Key Authentication
Advanced version with key-based security for production use
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import paramiko
from paramiko.ssh_exception import (
    AuthenticationException,
    SSHException,
    NoValidConnectionsError
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/downloads/sftp_client_key_auth.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SFTPClientKeyAuth:
    """SFTP client using SSH key authentication"""
    
    def __init__(self, host, port, username, private_key_path, 
                 passphrase=None):
        """
        Initialize SFTP client with key authentication
        
        Args:
            host: SFTP server hostname
            port: SFTP server port
            username: SSH username
            private_key_path: Path to private key file
            passphrase: Optional passphrase for encrypted key
        """
        self.host = host
        self.port = port
        self.username = username
        self.private_key_path = private_key_path
        self.passphrase = passphrase
        self.ssh_client = None
        self.sftp_client = None
        
    def load_private_key(self):
        """Load and return private key object"""
        try:
            # Determine key type
            with open(self.private_key_path, 'r') as f:
                key_data = f.read()
            
            if 'RSA' in key_data:
                key = paramiko.RSAKey.from_private_key_file(
                    self.private_key_path,
                    password=self.passphrase
                )
            elif 'ED25519' in key_data or 'OPENSSH' in key_data:
                key = paramiko.Ed25519Key.from_private_key_file(
                    self.private_key_path,
                    password=self.passphrase
                )
            else:
                # Try default RSA
                key = paramiko.RSAKey.from_private_key_file(
                    self.private_key_path,
                    password=self.passphrase
                )
            
            logger.info(f"Loaded private key: {self.private_key_path}")
            return key
            
        except paramiko.PasswordRequiredException:
            logger.error("Private key requires passphrase")
            raise
        except paramiko.SSHException as e:
            logger.error(f"Invalid private key: {e}")
            raise
        except FileNotFoundError:
            logger.error(f"Private key not found: {self.private_key_path}")
            raise
    
    def connect(self):
        """Connect using SSH key authentication"""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )
            
            private_key = self.load_private_key()
            
            logger.info(
                f"Connecting to {self.host}:{self.port} "
                f"as {self.username} (key auth)"
            )
            
            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                pkey=private_key,
                allow_agent=False,
                look_for_keys=False,
                timeout=10
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("Successfully connected to SFTP server (key auth)")
            return True
            
        except (AuthenticationException, SSHException, 
                NoValidConnectionsError) as e:
            logger.error(f"Connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False
    
    def disconnect(self):
        """Close connection"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            logger.info("Disconnected from SFTP server")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    def list_files(self, remote_path="."):
        """List remote files"""
        try:
            files = self.sftp_client.listdir_attr(remote_path)
            return [f for f in files if f.filename not in [".", ".."]]
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    def download_file(self, remote_path, local_path):
        """Download single file"""
        try:
            local_file = Path(local_path)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            self.sftp_client.get(remote_path, local_path)
            file_size = local_file.stat().st_size
            
            logger.info(
                f"Downloaded: {remote_path} → {local_path} "
                f"({file_size} bytes)"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            return False
    
    def download_directory(self, remote_dir, local_dir, recursive=True):
        """Download directory recursively"""
        try:
            local_path = Path(local_dir)
            local_path.mkdir(parents=True, exist_ok=True)
            
            files = self.list_files(remote_dir)
            downloaded_count = 0
            
            for file_attr in files:
                remote_file_path = f"{remote_dir}/{file_attr.filename}"
                local_file_path = local_path / file_attr.filename
                
                if file_attr.filename[0] != "-":
                    if recursive:
                        downloaded_count += self.download_directory(
                            remote_file_path,
                            str(local_file_path),
                            recursive=True
                        )
                else:
                    if self.download_file(remote_file_path, 
                                         str(local_file_path)):
                        downloaded_count += 1
            
            return downloaded_count
            
        except Exception as e:
            logger.error(f"Error downloading directory: {e}")
            return 0


def main():
    """Main application loop for key-based authentication"""
    
    # Configuration from environment
    sftp_host = os.getenv("SFTP_HOST", "sftp-server")
    sftp_port = int(os.getenv("SFTP_PORT", "22"))
    sftp_username = os.getenv("SFTP_USERNAME", "testuser")
    private_key = os.getenv("PRIVATE_KEY_PATH", "/app/keys/id_rsa")
    key_passphrase = os.getenv("KEY_PASSPHRASE", None)
    download_interval = int(os.getenv("DOWNLOAD_INTERVAL", "10"))
    remote_dir = os.getenv("REMOTE_DIR", "/upload")
    local_dir = os.getenv("LOCAL_DIR", "/app/downloads")
    
    logger.info("=" * 60)
    logger.info("SFTP Client (Key Authentication) Started")
    logger.info("=" * 60)
    
    sftp_client = SFTPClientKeyAuth(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        private_key_path=private_key,
        passphrase=key_passphrase
    )
    
    attempt = 0
    
    try:
        while True:
            attempt += 1
            logger.info(f"\n[Attempt {attempt}] "
                       f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            if sftp_client.connect():
                files = sftp_client.list_files(remote_dir)
                logger.info(f"Found {len(files)} file(s) on remote server")
                
                downloaded = sftp_client.download_directory(
                    remote_dir,
                    local_dir
                )
                
                if downloaded > 0:
                    logger.info(f"Downloaded {downloaded} file(s)")
                
                sftp_client.disconnect()
            else:
                logger.warning("Failed to connect. Retrying...")
            
            logger.info(f"Waiting {download_interval}s...")
            time.sleep(download_interval)
            
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
        sftp_client.disconnect()
        sys.exit(0)


if __name__ == "__main__":
    main()