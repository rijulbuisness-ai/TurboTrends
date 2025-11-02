#!/bin/bash

# Free AI News Bot Setup Script
# This script ensures you're in the correct directory and installs dependencies

echo "🤖 Free AI News Bot Setup"
echo "=========================="

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ requirements.txt not found in current directory"
    echo "📁 Current directory: $(pwd)"
    echo ""
    echo "Please navigate to the TurboTrends directory:"
    echo "cd TurboTrends"
    echo ""
    echo "Or run this setup script from within the TurboTrends directory."
    exit 1
fi

echo "✅ Found requirements.txt - you're in the correct directory!"
echo ""

# Check if Python is available
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "❌ Python not found. Please install Python 3.8+"
    exit 1
fi

# Use python3 if available, otherwise python
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    PYTHON_CMD="python"
fi

echo "📦 Installing dependencies..."
echo "Running: pip install -r requirements.txt"
echo ""

# Install dependencies
pip install -r requirements.txt

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Dependencies installed successfully!"
    echo ""
    echo "🧪 Testing basic functionality..."
    $PYTHON_CMD test_basic_modules.py

    if [ $? -eq 0 ]; then
        echo ""
        echo "🎉 Setup completed successfully!"
        echo ""
        echo "Next steps:"
        echo "1. Set up PostgreSQL database"
        echo "2. Configure .env file with database credentials"
        echo "3. Run migration: python migrate_database.py"
        echo "4. Start bot: python main.py"
    else
        echo ""
        echo "⚠️  Basic tests failed, but dependencies are installed."
        echo "   Check the error messages above for details."
    fi
else
    echo ""
    echo "❌ Failed to install dependencies"
    echo "Please check your internet connection and pip installation"
    exit 1
fi