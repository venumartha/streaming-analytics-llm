#!/bin/bash

echo "=========================================="
echo "Streaming Analytics Setup"
echo "=========================================="
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "✅ Docker and Docker Compose found"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.11+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo "✅ Found Python $PYTHON_VERSION"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo ""
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file
if [ ! -f .env ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  IMPORTANT: Edit .env and add your ANTHROPIC_API_KEY"
else
    echo ""
    echo "✅ .env file already exists"
fi

# Start Kafka infrastructure
echo ""
echo "Starting Kafka infrastructure with Docker Compose..."
docker-compose up -d

echo ""
echo "Waiting for Kafka to be ready (30 seconds)..."
sleep 30

# Check if Kafka is running
if docker-compose ps | grep -q "Up"; then
    echo "✅ Kafka infrastructure is running"
else
    echo "❌ Failed to start Kafka infrastructure"
    exit 1
fi

echo ""
echo "=========================================="
echo "Setup Complete! 🎉"
echo "=========================================="
echo ""
echo "Kafka UI: http://localhost:8080"
echo ""
echo "Next steps:"
echo "  1. Edit .env and add your ANTHROPIC_API_KEY"
echo "  2. Run the components in separate terminals:"
echo ""
echo "     Terminal 1 - Producer:"
echo "     source venv/bin/activate"
echo "     python producer.py --devices 10"
echo ""
echo "     Terminal 2 - Stream Processor:"
echo "     source venv/bin/activate"
echo "     python stream_processor.py"
echo ""
echo "     Terminal 3 - LLM Analyzer:"
echo "     source venv/bin/activate"
echo "     python llm_analyzer.py"
echo ""
echo "     Terminal 4 - API Server:"
echo "     source venv/bin/activate"
echo "     python api.py"
echo ""
echo "  3. View dashboard at http://localhost:8000"
echo ""
