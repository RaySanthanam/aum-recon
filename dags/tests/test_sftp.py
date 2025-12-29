"""Tests for SFTP module."""

from recon.sftp import download_dbf_files


def test_successful_download(monkeypatch):
    connected = [False]
    downloaded = [False]
    disconnected = [False]

    class MockSFTPClient:
        def __init__(self, host, port, username, password):
            self.host = host
            self.port = port

        def connect(self):
            connected[0] = True
            return True

        def download_dbf_files(self):
            downloaded[0] = True

        def disconnect(self):
            disconnected[0] = True

    monkeypatch.setattr("recon.sftp.SFTPClient", MockSFTPClient)

    download_dbf_files()

    assert connected[0]
    assert downloaded[0]
    assert disconnected[0]


def test_connection_failure(monkeypatch):
    connected = [False]
    downloaded = [False]

    class MockSFTPClient:
        def __init__(self, host, port, username, password):
            pass

        def connect(self):
            connected[0] = True
            return False

        def download_dbf_files(self):
            downloaded[0] = True

        def disconnect(self):
            pass

    monkeypatch.setattr("recon.sftp.SFTPClient", MockSFTPClient)

    download_dbf_files()

    assert connected[0]
    assert not downloaded[0]
