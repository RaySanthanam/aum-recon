#!/bin/bash

# SFTP Docker Project - Setup and Management Script

set -e

PROJECT_NAME="sftp-docker-project"
BOLD='\033[1m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}${BOLD}===================================================${NC}"
    echo -e "${BLUE}${BOLD}$1${NC}"
    echo -e "${BLUE}${BOLD}===================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

create_directories() {
    print_header "Creating Project Directories"
    mkdir -p data/sftp
    mkdir -p data/downloads
    print_success "Directories created"
    echo ""
}

build_images() {
    print_header "Building Docker Images"
    docker-compose build
    print_success "Docker images built successfully"
    echo ""
}

start_containers() {
    print_header "Starting Docker Containers"
    docker-compose up -d
    print_success "Containers started in detached mode"
    echo ""
}

stop_containers() {
    print_header "Stopping Docker Containers"
    docker-compose down
    print_success "Containers stopped"
    echo ""
}

view_logs() {
    print_header "Container Logs"
    docker-compose logs -f
}

view_sftp_logs() {
    docker-compose logs -f sftp-server
}

view_client_logs() {
    docker-compose logs -f python-client
}

list_remote_files() {
    print_header "Files on SFTP Server"
    docker exec sftp-client ls -lah /app/remote_files/ 2>/dev/null || echo "No files yet or directory not accessible"
    echo ""
}

list_downloaded_files() {
    print_header "Downloaded Files"
    docker exec sftp-client ls -lah /app/downloads/ 2>/dev/null || echo "No downloads yet"
    echo ""
}

test_connection() {
    print_header "Testing SFTP Connection"
    docker-compose exec python-client python -c "
import paramiko
import os

try:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname='sftp-server',
        port=22,
        username='testuser',
        password='testpass',
        timeout=5
    )
    sftp = ssh.open_sftp()
    files = sftp.listdir('.')
    print(f'✓ Connection successful! Remote files: {files}')
    sftp.close()
    ssh.close()
except Exception as e:
    print(f'✗ Connection failed: {e}')
    exit(1)
"
    echo ""
}

upload_test_file() {
    print_header "Uploading Test File to SFTP Server"
    TEST_FILE="data/sftp/test_$(date +%s).txt"
    echo "Test file created at $(date)" > "$TEST_FILE"
    print_success "Test file created: $TEST_FILE"
    echo ""
}

show_usage() {
    cat << EOF
${BOLD}SFTP Docker Project - Management Script${NC}

${BOLD}Usage:${NC}
    ./manage.sh <command>

${BOLD}Commands:${NC}
    build               Build Docker images
    start               Start all containers
    stop                Stop all containers
    logs                View all container logs
    logs-sftp           View SFTP server logs
    logs-client         View Python client logs
    test                Test SFTP connection
    files-remote        List files on SFTP server
    files-downloaded    List downloaded files
    upload-test         Create and upload test file
    create-dirs         Create project directories
    rebuild             Clean, rebuild, and start fresh
    clean               Remove containers and volumes

${BOLD}Examples:${NC}
    ./manage.sh build
    ./manage.sh start
    ./manage.sh logs
    ./manage.sh test

EOF
}

clean_all() {
    print_header "Cleaning Up (Removing Containers and Volumes)"
    docker-compose down -v
    print_success "Cleanup complete"
    echo ""
}

rebuild() {
    print_header "Rebuilding Project"
    clean_all
    create_directories
    build_images
    start_containers
    print_success "Project rebuilt and started"
    echo ""
}

# Main script logic
case "$1" in
    build)
        build_images
        ;;
    start)
        start_containers
        ;;
    stop)
        stop_containers
        ;;
    logs)
        view_logs
        ;;
    logs-sftp)
        view_sftp_logs
        ;;
    logs-client)
        view_client_logs
        ;;
    test)
        test_connection
        ;;
    files-remote)
        list_remote_files
        ;;
    files-downloaded)
        list_downloaded_files
        ;;
    upload-test)
        upload_test_file
        ;;
    create-dirs)
        create_directories
        ;;
    rebuild)
        rebuild
        ;;
    clean)
        clean_all
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
