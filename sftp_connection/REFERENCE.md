# SFTP Docker Project - Complete Reference

## Project Overview

A production-ready SFTP server and Python client setup running in Docker. The architecture consists of:

- **SFTP Server**: Using `atmoz/sftp` image with OpenSSH
- **Python Client**: Custom application using Paramiko library
- **Docker Compose**: Orchestrates both services on a private bridge network
- **Volume Mounts**: Persistent storage for uploaded and downloaded files

## Files Included

### Core Configuration

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Main orchestration configuration |
| `Dockerfile.client` | Python client Docker image definition |
| `requirements.txt` | Python dependencies (Paramiko, Cryptography) |
| `.gitignore` | Git ignore patterns |

### Application Code

| File | Purpose |
|------|---------|
| `sftp_client.py` | Main SFTP client with password authentication |
| `sftp_client_key_auth.py` | Advanced client with SSH key authentication |

### Documentation & Scripts

| File | Purpose |
|------|---------|
| `README.md` | Complete documentation |
| `QUICKSTART.md` | 5-minute quick start guide |
| `REFERENCE.md` | This file - comprehensive reference |
| `manage.sh` | CLI management script for common operations |
| `setup.sh` | Initial setup script (one-time) |

### Directories (Created)

```
data/
├── sftp/           # SFTP server storage (upload location)
├── downloads/      # Downloaded files location
└── logs/           # Application logs
```

## Quick Command Reference

```bash
# Initial setup
chmod +x setup.sh
./setup.sh

# Build and start
./manage.sh create-dirs
./manage.sh build
./manage.sh start

# Operations
./manage.sh test              # Test SFTP connection
./manage.sh upload-test       # Create test file
./manage.sh files-remote      # List remote files
./manage.sh files-downloaded  # List downloaded files

# Monitoring
./manage.sh logs              # All logs
./manage.sh logs-sftp         # SFTP server logs
./manage.sh logs-client       # Python client logs

# Maintenance
./manage.sh stop              # Stop containers
./manage.sh rebuild           # Clean rebuild
./manage.sh clean             # Full cleanup
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│          Docker Network: sftp-network               │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────────┐      ┌──────────────────┐   │
│  │  SFTP Server     │      │  Python Client   │   │
│  ├──────────────────┤      ├──────────────────┤   │
│  │ atmoz/sftp image │      │ Python 3.11      │   │
│  │ Port: 22         │      │ Paramiko         │   │
│  │ User: testuser   │◄────►│ Every 10s        │   │
│  └──────────────────┘      └──────────────────┘   │
│       ▲                            ▲               │
│       │                            │               │
│       │ Volume Mount               │ Volume Mount  │
│       │                            │               │
└───────┼────────────────────────────┼───────────────┘
        │                            │
        │                            │
    data/sftp              data/downloads
    (Uploads)              (Downloads)
```

## Configuration Reference

### docker-compose.yml - SFTP Server

```yaml
sftp-server:
  image: atmoz/sftp                    # Official SFTP image
  container_name: sftp-server
  ports:
    - "2222:22"                        # External port: internal port
  environment:
    - SFTP_USERS=testuser:testpass:1001
      # Format: username:password:uid
      # Multiple users: user1:pass1:1001|user2:pass2:1002
  volumes:
    - ./data/sftp:/home/testuser/upload # Mount for file storage
  restart: unless-stopped              # Auto-restart on failure
```

### docker-compose.yml - Python Client

```yaml
python-client:
  build:
    context: .
    dockerfile: Dockerfile.client
  container_name: sftp-client
  depends_on:
    - sftp-server                      # Start after SFTP server
  environment:
    - SFTP_HOST=sftp-server           # Hostname (inside network)
    - SFTP_PORT=22                    # Port (inside container)
    - SFTP_USERNAME=testuser
    - SFTP_PASSWORD=testpass
    - DOWNLOAD_INTERVAL=10            # Seconds between downloads
    - REMOTE_DIR=/upload              # Server directory to download
    - LOCAL_DIR=/app/downloads        # Container directory for downloads
  volumes:
    - ./data/downloads:/app/downloads  # Persist downloads
  restart: unless-stopped
```

## Environment Variables

### Client Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SFTP_HOST` | sftp-server | SFTP server hostname |
| `SFTP_PORT` | 22 | SFTP server port |
| `SFTP_USERNAME` | testuser | SFTP username |
| `SFTP_PASSWORD` | testpass | SFTP password |
| `DOWNLOAD_INTERVAL` | 10 | Seconds between download attempts |
| `REMOTE_DIR` | /upload | Remote directory to download from |
| `LOCAL_DIR` | /app/downloads | Local directory to save files |

## API Documentation

### SFTPClient Class (sftp_client.py)

```python
from sftp_connection.sftp_client import SFTPClient

# Initialize
client = SFTPClient(
    host="sftp-server",
    port=22,
    username="testuser",
    password="testpass"
)

# Connect
if client.connect():
    # List files
    files = client.list_files("/upload")

    # Download file
    client.download_file("/upload/file.txt", "/local/path/file.txt")

    # Download directory
    count = client.download_directory("/upload", "/local/dir")

    # Disconnect
    client.disconnect()
```

### SFTPClientKeyAuth Class (sftp_client_key_auth.py)

```python
from sftp_connection.sftp_client_key_auth import SFTPClientKeyAuth

# Initialize with SSH key
client = SFTPClientKeyAuth(
    host="sftp-server",
    port=22,
    username="testuser",
    private_key_path="/path/to/id_rsa",
    passphrase="optional_passphrase"
)

# Same methods as SFTPClient above
if client.connect():
    files = client.list_files()
    client.disconnect()
```

## Common Scenarios

### Scenario 1: Development Testing

**Setup**: Fast iteration with short download intervals

```yaml
environment:
  - DOWNLOAD_INTERVAL=5  # Check every 5 seconds
```

**Usage**:
```bash
./manage.sh upload-test
./manage.sh logs-client
# Watch real-time downloads
```

### Scenario 2: Production Deployment

**Setup**: Robust configuration with SSH keys

Edit `docker-compose.yml`:
```yaml
environment:
  - SFTP_HOST=prod-sftp.example.com
  - SFTP_PORT=2222
  - DOWNLOAD_INTERVAL=300  # Check every 5 minutes
```

Use SSH key authentication:
```yaml
build:
  dockerfile: Dockerfile.client-keys
environment:
  - PRIVATE_KEY_PATH=/secrets/id_rsa
volumes:
  - /secure/keys:/secrets:ro
```

### Scenario 3: Backup Integration

**Goal**: Daily backup downloads

```yaml
environment:
  - DOWNLOAD_INTERVAL=86400  # 24 hours
  - REMOTE_DIR=/backups
```

### Scenario 4: Real-Time File Sync

**Goal**: Continuous monitoring

```yaml
environment:
  - DOWNLOAD_INTERVAL=1  # Every second
```

**Note**: Use with caution - may impact performance

## Logging

### Log Locations

**Inside Container**:
- `sftp_client.log`: `/app/downloads/sftp_client.log`
- Docker logs: `docker logs sftp-client`

**On Host Machine**:
- `data/downloads/sftp_client.log`

### Log Format

```
TIMESTAMP - LOGGER_NAME - LEVEL - MESSAGE
2025-12-18 19:30:15,123 - __main__ - INFO - Successfully connected to SFTP server
```

### Log Levels

| Level | Meaning |
|-------|---------|
| DEBUG | Detailed diagnostic info |
| INFO | Informational messages (default) |
| WARNING | Warning conditions |
| ERROR | Error conditions |
| CRITICAL | Critical conditions |

### View Logs

```bash
# Real-time logs
docker-compose logs -f

# Last 50 lines
docker-compose logs --tail=50

# Specific service
docker-compose logs sftp-server

# With timestamps
docker-compose logs -t
```

## Troubleshooting Guide

### Connection Issues

**Problem**: "Cannot connect to SFTP server"

**Diagnosis**:
```bash
# Check if SFTP server is running
docker ps | grep sftp-server

# Check server logs
./manage.sh logs-sftp

# Test port
docker run --rm -it alpine telnet sftp-server 22
```

**Solution**:
- Verify SFTP_HOST matches container name
- Check SFTP_PORT (usually 22 internally, 2222 externally)
- Restart containers: `./manage.sh rebuild`

### Authentication Failure

**Problem**: "Authentication failed"

**Diagnosis**:
```bash
# Check credentials in docker-compose.yml
# Verify SFTP_USERS matches SFTP_USERNAME:SFTP_PASSWORD
```

**Solution**:
```bash
# Update docker-compose.yml with correct credentials
# Rebuild
./manage.sh rebuild
```

### Files Not Downloading

**Problem**: Downloaded files directory is empty

**Diagnosis**:
```bash
# Check remote directory
docker exec sftp-server ls -la /home/testuser/upload/

# Check client logs
./manage.sh logs-client

# Check permissions
docker exec sftp-client ls -la /app/downloads/
```

**Solution**:
- Upload test file: `./manage.sh upload-test`
- Verify REMOTE_DIR setting
- Check file permissions (chmod +r files)

### Port Conflicts

**Problem**: "Address already in use"

**Solution**:
1. Change port in `docker-compose.yml`:
```yaml
ports:
  - "2223:22"  # Use 2223 instead of 2222
```

2. Or kill existing process:
```bash
lsof -i :2222
kill -9 <PID>
```

## Performance Optimization

### High Volume Downloads

```yaml
environment:
  - DOWNLOAD_INTERVAL=300  # 5 minutes
  - BUFFER_SIZE=65536     # Larger buffer (if supported)
```

### Low Latency

```yaml
environment:
  - DOWNLOAD_INTERVAL=1   # Every second
```

### Resource Limits

```yaml
services:
  python-client:
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 256M
```

## Security Best Practices

### 1. Use SSH Keys Instead of Passwords

See `sftp_client_key_auth.py` for implementation

### 2. Use Non-Root Users

Always configure SFTP with specific UID:
```yaml
SFTP_USERS=sftpuser:pass:1001  # Not 0 or 1000
```

### 3. Restrict Directory Access

In SFTP server configuration:
```
ChrootDirectory /home/sftpuser/uploads
ForceCommand internal-sftp
```

### 4. Enable Host Key Verification

In production, replace `AutoAddPolicy()`:
```python
ssh.load_system_host_keys()
ssh.connect(hostname, username=user, pkey=key, 
           look_for_keys=True)
```

### 5. Use Secrets Management

For passwords and keys:
```bash
# Docker Secrets (Swarm mode)
docker secret create sftp_password passwords.txt

# Or environment file
docker-compose --env-file .env.production
```

## Monitoring & Health Checks

### Add Health Check to docker-compose.yml

```yaml
services:
  sftp-server:
    healthcheck:
      test: ["CMD", "test", "-f", "/home/testuser/upload"]
      interval: 30s
      timeout: 10s
      retries: 3
  
  python-client:
    healthcheck:
      test: ["CMD", "test", "-f", "/app/downloads/sftp_client.log"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Monitor Container Status

```bash
# See health status
docker ps --format "table {{.Names}}\t{{.Status}}"

# Watch in real-time
watch -n 1 'docker ps --format "table {{.Names}}\t{{.Status}}"'
```

## Advanced Customization

### Custom File Processing

Extend `SFTPClient`:
```python
class CustomSFTPClient(SFTPClient):
    def process_file(self, local_path):
        # Custom processing logic
        if local_path.endswith('.zip'):
            self.extract_zip(local_path)
        elif local_path.endswith('.json'):
            self.validate_json(local_path)
```

### Webhook Notifications

```python
import requests

def notify_download_complete(filename, size):
    requests.post('https://webhook.site/xxx', 
        json={
            'filename': filename,
            'size': size,
            'timestamp': datetime.now().isoformat()
        }
    )
```

### Database Logging

```python
import sqlite3

def log_download_to_db(filename, size, status):
    conn = sqlite3.connect('/app/downloads/transfer.db')
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO transfers VALUES (?, ?, ?, ?)',
        (filename, size, status, datetime.now())
    )
    conn.commit()
```

## Deployment to Cloud

### Docker Hub

```bash
# Build and push
docker tag sftp-client:latest myrepo/sftp-client:1.0
docker push myrepo/sftp-client:1.0
```

### AWS ECR

```bash
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker tag sftp-client:latest <account>.dkr.ecr.<region>.amazonaws.com/sftp-client:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/sftp-client:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sftp-client
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: client
        image: sftp-client:latest
        env:
        - name: SFTP_HOST
          value: "sftp-server.default.svc.cluster.local"
        volumeMounts:
        - name: downloads
          mountPath: /app/downloads
      volumes:
      - name: downloads
        persistentVolumeClaim:
          claimName: downloads-pvc
```

## Support & Resources

- **Paramiko Docs**: https://www.paramiko.org/
- **Docker Docs**: https://docs.docker.com/
- **atmoz/sftp**: https://github.com/atmoz/sftp
- **SFTP RFC**: https://tools.ietf.org/html/rfc4251

## Version History

- **v1.0** (Dec 2025): Initial release with password auth, Docker Compose setup, management scripts
- **v1.1** (Planned): SSH key authentication support
- **v1.2** (Planned): Kubernetes deployment examples
- **v2.0** (Planned): Web UI for monitoring, database backend

---

**Last Updated**: December 18, 2025  
**Compatibility**: macOS, Linux, Windows (with Docker Desktop)  
**Tested On**: Docker 24.0+, Docker Compose 2.0+