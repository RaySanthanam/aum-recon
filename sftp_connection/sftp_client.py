#!/usr/bin/env python3
"""
SFTP Client Application
Downloads files from SFTP server at regular intervals
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/downloads/sftp_client.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SFTPClient:
    """Handles SFTP connection and file operations"""
    
    def __init__(self, host, port, username, password):
        """Initialize SFTP client with connection parameters"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssh_client = None
        self.sftp_client = None
        
    def connect(self):
        """Establish SFTP connection"""
        try:
            self.ssh_client = paramiko.SSHClient()
            # Automatically add unknown host keys
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )
            
            logger.info(
                f"Connecting to SFTP server: {self.host}:{self.port} "
                f"as {self.username}"
            )
            
            self.ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                allow_agent=False,
                look_for_keys=False,
                timeout=10
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("Successfully connected to SFTP server")
            return True
            
        except AuthenticationException as e:
            logger.error(f"Authentication failed: {e}")
            return False
        except (SSHException, NoValidConnectionsError) as e:
            logger.error(f"SSH connection error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            return False
    
    def disconnect(self):
        """Close SFTP connection"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
            if self.ssh_client:
                self.ssh_client.close()
            logger.info("Disconnected from SFTP server")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    def list_files(self, remote_path="."):
        """List files in remote directory"""
        try:
            files = self.sftp_client.listdir_attr(remote_path)
            return [f for f in files if f.filename not in [".", ".."]]
        except Exception as e:
            logger.error(f"Error listing files in {remote_path}: {e}")
            return []
    
    def download_file(self, remote_path, local_path):
        """Download single file from SFTP server"""
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
            
        except FileNotFoundError as e:
            logger.error(f"File not found on remote server: {remote_path}")
            return False
        except Exception as e:
            logger.error(
                f"Error downloading {remote_path} to {local_path}: {e}"
            )
            return False
    
    def download_directory(self, remote_dir, local_dir, recursive=True):
        """Download directory from SFTP server"""
        try:
            local_path = Path(local_dir)
            local_path.mkdir(parents=True, exist_ok=True)
            
            files = self.list_files(remote_dir)
            downloaded_count = 0
            
            for file_attr in files:
                remote_file_path = f"{remote_dir}/{file_attr.filename}"
                local_file_path = local_path / file_attr.filename
                
                # Check if it's a directory
                if file_attr.filename[0] != "-":  # Not a regular file
                    if recursive:
                        downloaded_count += self.download_directory(
                            remote_file_path,
                            str(local_file_path),
                            recursive=True
                        )
                else:
                    # Download file
                    if self.download_file(remote_file_path, str(local_file_path)):
                        downloaded_count += 1
            
            return downloaded_count
            
        except Exception as e:
            logger.error(f"Error downloading directory {remote_dir}: {e}")
            return 0


def download_latest_files(sftp_client, remote_dir, local_dir):
    """
    Download new/updated files from remote directory
    """
    try:
        downloaded = sftp_client.download_directory(remote_dir, local_dir)
        if downloaded > 0:
            logger.info(
                f"Successfully downloaded {downloaded} file(s) "
                f"to {local_dir}"
            )
        else:
            logger.info("No new files to download")
        return downloaded
    except Exception as e:
        logger.error(f"Error during download process: {e}")
        return 0


def main():
    """Main application loop"""
    
    # Get configuration from environment variables
    sftp_host = os.getenv("SFTP_HOST", "sftp-server")
    sftp_port = int(os.getenv("SFTP_PORT", "22"))
    sftp_username = os.getenv("SFTP_USERNAME", "testuser")
    sftp_password = os.getenv("SFTP_PASSWORD", "testpass")
    download_interval = int(os.getenv("DOWNLOAD_INTERVAL", "10"))
    remote_dir = os.getenv("REMOTE_DIR", "/upload")
    local_dir = os.getenv("LOCAL_DIR", "/app/downloads")
    
    logger.info("=" * 60)
    logger.info("SFTP Client Started")
    logger.info("=" * 60)
    logger.info(f"Configuration:")
    logger.info(f"  SFTP Server: {sftp_host}:{sftp_port}")
    logger.info(f"  Username: {sftp_username}")
    logger.info(f"  Download Interval: {download_interval} seconds")
    logger.info(f"  Remote Directory: {remote_dir}")
    logger.info(f"  Local Directory: {local_dir}")
    logger.info("=" * 60)
    
    # Initialize SFTP client
    sftp_client = SFTPClient(
        host=sftp_host,
        port=sftp_port,
        username=sftp_username,
        password=sftp_password
    )
    
    # Main loop
    download_attempt = 0
    
    try:
        while True:
            download_attempt += 1
            
            logger.info(f"\n[Attempt {download_attempt}] "
                       f"Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Connect to SFTP server
            if sftp_client.connect():
                # Perform download
                download_latest_files(
                    sftp_client,
                    remote_dir,
                    local_dir
                )
                
                # Disconnect
                sftp_client.disconnect()
            else:
                logger.warning("Failed to connect to SFTP server. "
                             "Retrying in next interval...")
            
            # Wait before next attempt
            logger.info(f"Waiting {download_interval} seconds before next check...")
            time.sleep(download_interval)
            
    except KeyboardInterrupt:
        logger.info("\nShutdown signal received")
        sftp_client.disconnect()
        logger.info("Application terminated gracefully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        sftp_client.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()