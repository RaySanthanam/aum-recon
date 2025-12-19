#!/bin/bash

# Initial Project Setup Script
# Run this after extracting the project for first-time setup

set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}${BOLD}========================================${NC}"
echo -e "${BLUE}${BOLD}SFTP Docker Project - Initial Setup${NC}"
echo -e "${BLUE}${BOLD}========================================${NC}\n"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    echo "Please install Docker from: https://docs.docker.com/get-docker/"
    exit 1
fi

echo -e "${GREEN}✓ Docker found${NC}"

# Check if Docker daemon is running
if ! docker ps &> /dev/null; then
    echo -e "${RED}✗ Docker daemon is not running${NC}"
    echo "Please start Docker and try again"
    exit 1
fi

echo -e "${GREEN}✓ Docker daemon is running${NC}\n"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${YELLOW}⚠ docker-compose not found${NC}"
    echo "Checking for docker compose plugin..."
    
    if ! docker compose version &> /dev/null; then
        echo -e "${RED}✗ Docker Compose not available${NC}"
        exit 1
    fi
    
    # Create docker-compose alias if using plugin
    if [ ! -f "docker-compose" ]; then
        echo -e "${YELLOW}⚠ Creating docker-compose wrapper${NC}"
        cat > docker-compose << 'EOF'
#!/bin/bash
docker compose "$@"
EOF
        chmod +x docker-compose
    fi
fi

echo -e "${GREEN}✓ Docker Compose is available${NC}\n"

# Make scripts executable
echo -e "${BLUE}Making scripts executable...${NC}"
chmod +x manage.sh
echo -e "${GREEN}✓ Scripts are executable${NC}\n"

# Create directories
echo -e "${BLUE}Creating project directories...${NC}"
mkdir -p data/sftp
mkdir -p data/downloads
mkdir -p data/logs
echo -e "${GREEN}✓ Directories created${NC}\n"

# Display next steps
echo -e "${GREEN}${BOLD}========================================${NC}"
echo -e "${GREEN}${BOLD}Setup Complete!${NC}"
echo -e "${GREEN}${BOLD}========================================${NC}\n"

echo -e "${BOLD}Next Steps:${NC}\n"
echo "1. Read the documentation:"
echo -e "   ${BLUE}cat QUICKSTART.md${NC}\n"

echo "2. Build Docker images:"
echo -e "   ${BLUE}./manage.sh build${NC}\n"

echo "3. Start services:"
echo -e "   ${BLUE}./manage.sh start${NC}\n"

echo "4. Verify setup:"
echo -e "   ${BLUE}./manage.sh test${NC}\n"

echo "5. View logs:"
echo -e "   ${BLUE}./manage.sh logs${NC}\n"

echo -e "${YELLOW}For more commands:${NC}"
echo -e "   ${BLUE}./manage.sh${NC}\n"

echo -e "${GREEN}✓ Your SFTP project is ready!${NC}"
echo -e "${GREEN}Happy coding! 🚀${NC}\n"