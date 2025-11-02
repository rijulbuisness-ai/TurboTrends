#!/usr/bin/env python3
"""
Simple runner script for the Free AI News Bot
This script ensures everything runs correctly regardless of current directory
"""

import os
import sys

# Add the current script's directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def run_bot():
    """Run the free AI news bot"""
    print("🤖 Free AI News Bot Starting...")
    print(f"📁 Working directory: {current_dir}")

    # Check if main.py exists
    main_file = os.path.join(current_dir, 'main.py')
    if not os.path.exists(main_file):
        print(f"❌ main.py not found at {main_file}")
        return False

    print("✅ Found main.py")

    # Check if .env exists
    env_file = os.path.join(current_dir, '.env')
    if not os.path.exists(env_file):
        print(f"❌ .env file not found at {env_file}")
        print("Please configure your database settings in .env")
        return False

    print("✅ Found .env configuration")

    try:
        # Import and run the bot
        print("🚀 Starting main bot...")
        import main
        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure dependencies are installed:")
        print("pip install -r requirements.txt")
        return False

    except Exception as e:
        print(f"❌ Runtime error: {e}")
        return False

if __name__ == "__main__":
    success = run_bot()
    if not success:
        print("\n💡 Quick help:")
        print("1. Make sure you're in the TurboTrends directory")
        print("2. Install dependencies: pip install -r requirements.txt")
        print("3. Configure .env with database settings")
        print("4. Run database migration: python migrate_database.py")
        sys.exit(1)