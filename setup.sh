#!/bin/bash

# Metis Application Setup Script
# This script sets up the complete Metis application stack

set -e  # Exit on any error

echo "🚀 Setting up Metis Application..."
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "package.json" ] || [ ! -d "server" ] || [ ! -d "client" ]; then
    print_error "Please run this script from the metis-mono-repo root directory"
    exit 1
fi

print_status "Checking prerequisites..."

# Check for Node.js
if ! command -v node &> /dev/null; then
    print_error "Node.js is not installed. Please install Node.js 18+ and try again."
    exit 1
fi

# Check for Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8+ and try again."
    exit 1
fi

# Check for pip
if ! command -v pip &> /dev/null && ! command -v pip3 &> /dev/null; then
    print_error "pip is not installed. Please install pip and try again."
    exit 1
fi

print_success "Prerequisites check passed!"

# 1. SERVER SETUP
print_status "Setting up MCP Server..."
echo "----------------------------------------"

cd server

print_status "Installing server dependencies..."
npm install

print_status "Building server..."
npm run build

print_status "Setting up MCP registry (indexing + AI summaries)..."
if npm run setup-registry; then
    print_success "MCP registry setup completed successfully"
else
    print_warning "Registry setup completed with warnings (check output above)"
    print_warning "The application will still work with basic functionality"
fi

print_success "Server setup complete!"

cd ..

# 2. BACKEND SETUP
print_status "Setting up Python Backend..."
echo "----------------------------------------"

cd client/backend

print_status "Installing Python dependencies..."
# Use pip3 if pip is not available
if command -v pip3 &> /dev/null; then
    pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
else
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
fi

print_success "Backend setup complete!"

cd ../..

# 3. FRONTEND SETUP
print_status "Setting up Frontend..."
echo "----------------------------------------"

cd client/frontend

print_status "Installing frontend dependencies..."
npm install

print_status "Building frontend..."
npm run build

print_success "Frontend setup complete!"

cd ../..

# 4. FINAL SETUP
print_success "🎉 Metis Application setup complete!"
echo ""
echo "📋 NEXT STEPS:"
echo "=============="
echo ""
echo "1. Configure your MCP servers in server/mcp-registry.json"
echo "2. Set up environment variables (.env files) as needed"
echo "3. Start the application using one of these methods:"
echo ""
echo "   🔧 DEVELOPMENT MODE (3 separate terminals):"
echo "   ./start.sh --separate-terminals"
echo ""
echo "   🚀 BACKGROUND MODE (single terminal):"
echo "   ./start.sh"
echo ""
echo "   📖 MANUAL MODE (3 separate terminals):"
echo "   Terminal 1: cd server && npm run dev:http"
echo "   Terminal 2: cd client/backend && uvicorn app:app --host localhost --port 8000 --log-level debug"
echo "   Terminal 3: cd client/frontend && npm start"
echo ""
echo "4. Open http://localhost:3000 in your browser"
echo ""
echo "📚 For more information, see README.md"
echo ""
print_warning "Note: Make sure to configure your environment variables and MCP registry before starting!" 