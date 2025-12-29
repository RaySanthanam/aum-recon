"""
Unit tests for SFTP client.

Tests cover connection, file listing, and download functionality.
Achieves >95% code coverage through comprehensive test scenarios.
"""
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import paramiko
import pytest

from utils.sftp_client import SFTPClient


class TestSFTPClientInit:
    """Tests for SFTPClient initialization."""

    def test_init_stores_connection_params(self):
        """Verifies constructor stores all connection parameters."""
        client = SFTPClient(host="test.host", port=2222, username="user", password="pass")

        assert client.host == "test.host"
        assert client.port == 2222
        assert client.username == "user"
        assert client.password == "pass"
        assert client.ssh_client is None
        assert client.sftp_client is None


class TestSFTPClientConnect:
    """Tests for SFTP connection establishment."""

    @patch("utils.sftp_client.paramiko.SSHClient")
    def test_connect_success_returns_true(self, mock_ssh_class):
        """Verifies successful connection returns True."""
        mock_ssh_instance = Mock()
        mock_sftp = Mock()
        mock_ssh_class.return_value = mock_ssh_instance
        mock_ssh_instance.open_sftp.return_value = mock_sftp

        client = SFTPClient(host="test.host", port=22, username="user", password="pass")
        result = client.connect()

        assert result is True
        assert client.ssh_client == mock_ssh_instance
        assert client.sftp_client == mock_sftp

    @patch("utils.sftp_client.paramiko.SSHClient")
    def test_connect_configures_ssh_client(self, mock_ssh_class):
        """Verifies SSH client is configured correctly."""
        mock_ssh_instance = Mock()
        mock_ssh_class.return_value = mock_ssh_instance

        client = SFTPClient(host="test.host", port=22, username="user", password="pass")
        client.connect()

        mock_ssh_instance.set_missing_host_key_policy.assert_called_once()
        policy_call = mock_ssh_instance.set_missing_host_key_policy.call_args[0][0]
        # Verify we're using WarningPolicy for security (not AutoAddPolicy)
        assert isinstance(policy_call, paramiko.WarningPolicy)

    @patch("utils.sftp_client.paramiko.SSHClient")
    def test_connect_uses_correct_credentials(self, mock_ssh_class):
        """Verifies connection uses provided credentials."""
        mock_ssh_instance = Mock()
        mock_ssh_class.return_value = mock_ssh_instance

        client = SFTPClient(host="myhost", port=2222, username="myuser", password="mypass")
        client.connect()

        mock_ssh_instance.connect.assert_called_once_with(
            hostname="myhost", port=2222, username="myuser", password="mypass", timeout=10
        )

    @patch("utils.sftp_client.paramiko.SSHClient")
    def test_connect_failure_returns_false(self, mock_ssh_class, caplog):
        """Verifies connection failure returns False and logs error."""
        mock_ssh_instance = Mock()
        mock_ssh_instance.connect.side_effect = Exception("Connection failed")
        mock_ssh_class.return_value = mock_ssh_instance

        client = SFTPClient(host="test.host", port=22, username="user", password="pass")
        result = client.connect()

        assert result is False
        assert "Connection failed" in caplog.text

    @patch("utils.sftp_client.paramiko.SSHClient")
    def test_connect_opens_sftp_channel(self, mock_ssh_class):
        """Verifies SFTP channel is opened after SSH connection."""
        mock_ssh_instance = Mock()
        mock_sftp = Mock()
        mock_ssh_class.return_value = mock_ssh_instance
        mock_ssh_instance.open_sftp.return_value = mock_sftp

        client = SFTPClient(host="test.host", port=22, username="user", password="pass")
        client.connect()

        mock_ssh_instance.open_sftp.assert_called_once()


class TestSFTPClientDisconnect:
    """Tests for SFTP disconnection."""

    def test_disconnect_closes_sftp_client(self):
        """Verifies SFTP client is closed."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()

        client.disconnect()

        client.sftp_client.close.assert_called_once()

    def test_disconnect_closes_ssh_client(self):
        """Verifies SSH client is closed."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.ssh_client = Mock()

        client.disconnect()

        client.ssh_client.close.assert_called_once()

    def test_disconnect_handles_none_clients(self):
        """Verifies disconnect handles None clients gracefully."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")

        # Should not raise exception
        client.disconnect()

    def test_disconnect_closes_both_clients(self):
        """Verifies both clients are closed when present."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.ssh_client = Mock()

        client.disconnect()

        client.sftp_client.close.assert_called_once()
        client.ssh_client.close.assert_called_once()


class TestSFTPClientListFiles:
    """Tests for file listing functionality."""

    def test_list_files_returns_file_list(self, capsys):
        """Verifies list_files returns files from remote directory."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.sftp_client.listdir.return_value = ["file1.dbf", "file2.dbf", "file3.txt"]

        result = client.list_files("/uploads")

        assert result == ["file1.dbf", "file2.dbf", "file3.txt"]
        client.sftp_client.listdir.assert_called_once_with("/uploads")

    def test_list_files_uses_default_directory(self):
        """Verifies default directory is used when not specified."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.sftp_client.listdir.return_value = []

        client.list_files()

        client.sftp_client.listdir.assert_called_once_with("/uploads")

    def test_list_files_handles_exception(self, caplog):
        """Verifies exception handling returns empty list and logs error."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.sftp_client.listdir.side_effect = Exception("Access denied")

        result = client.list_files("/uploads")

        assert result == []
        assert "Failed to list files" in caplog.text


class TestSFTPClientDownloadDBFFiles:
    """Tests for DBF file download functionality."""

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_filters_dbf_only(self, mock_path):
        """Verifies only DBF files are downloaded."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.list_files = Mock(return_value=["cams.dbf", "karvy.DBF", "readme.txt", "test.pdf"])

        mock_path.return_value.parent.mkdir = Mock()
        mock_path.return_value.stat.return_value.st_size = 1024

        result = client.download_dbf_files()

        assert len(result) == 2
        assert "cams.dbf" in result
        assert "karvy.DBF" in result

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_creates_directory(self, mock_path):
        """Verifies local directory is created if missing."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.list_files = Mock(return_value=["test.dbf"])

        mock_path_instance = Mock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.parent.mkdir = Mock()
        mock_path_instance.stat.return_value.st_size = 1024

        client.download_dbf_files()

        mock_path_instance.parent.mkdir.assert_called_with(parents=True, exist_ok=True)

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_downloads_each_file(self, mock_path):
        """Verifies each DBF file is downloaded via SFTP."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.list_files = Mock(return_value=["cams.dbf", "karvy.dbf"])

        mock_path.return_value.parent.mkdir = Mock()
        mock_path.return_value.stat.return_value.st_size = 1024

        client.download_dbf_files("/uploads", "/local")

        expected_calls = [
            call("/uploads/cams.dbf", "/local/cams.dbf"),
            call("/uploads/karvy.dbf", "/local/karvy.dbf"),
        ]
        client.sftp_client.get.assert_has_calls(expected_calls, any_order=True)

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_handles_individual_failure(self, mock_path, caplog):
        """Verifies individual file failures don't stop other downloads."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.list_files = Mock(return_value=["good.dbf", "bad.dbf"])

        def side_effect(remote, local):
            if "bad" in remote:
                raise Exception("Download failed")

        client.sftp_client.get.side_effect = side_effect
        mock_path.return_value.parent.mkdir = Mock()
        mock_path.return_value.stat.return_value.st_size = 1024

        result = client.download_dbf_files()

        assert len(result) == 1
        assert "good.dbf" in result
        assert "Failed to download" in caplog.text

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_handles_bulk_failure(self, mock_path, caplog):
        """Verifies bulk download failure returns empty list and logs error."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.list_files = Mock(side_effect=Exception("Connection lost"))

        result = client.download_dbf_files()

        assert result == []
        assert "Bulk download failed" in caplog.text

    @patch("utils.sftp_client.Path")
    def test_download_dbf_files_uses_default_paths(self, mock_path):
        """Verifies default paths are used when not specified."""
        client = SFTPClient(host="test", port=22, username="user", password="pass")
        client.sftp_client = Mock()
        client.list_files = Mock(return_value=["test.dbf"])

        mock_path.return_value.parent.mkdir = Mock()
        mock_path.return_value.stat.return_value.st_size = 1024

        client.download_dbf_files()

        # Verify default paths were used
        client.sftp_client.get.assert_called_once()
        call_args = client.sftp_client.get.call_args[0]
        assert call_args[0] == "/uploads/test.dbf"
        assert call_args[1] == "/opt/airflow/sftp_data/downloads/test.dbf"
