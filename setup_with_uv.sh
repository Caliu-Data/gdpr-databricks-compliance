#!/bin/bash
# Setup script using uv package manager

set -e

echo "🚀 Setting up GDPR Compliance Framework with uv..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

echo "✅ uv is installed"

# Create virtual environment
echo "📦 Creating virtual environment..."
uv venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
uv pip install -r requirements.txt

# Install package in development mode
echo "🔧 Installing package in development mode..."
uv pip install -e .

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Copy env.example to .env: cp env.example .env"
echo "2. Edit .env with your Databricks credentials"
echo "3. Generate encryption keys: openssl rand -hex 32"
echo "4. Run: python deploy.py"
echo ""

