#!/usr/bin/env python3
"""
Quick test script that works from any directory
Tests the core functionality of the Free AI News Bot
"""

import os
import sys

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_basic_functionality():
    """Test basic functionality without heavy dependencies"""
    print("🧪 Quick Test for Free AI News Bot")
    print("=" * 40)

    tests_passed = 0
    tests_failed = 0

    # Test 1: Check files exist
    print("\n1. Checking required files...")
    required_files = ['news_fetcher.py', 'summarizer.py', 'deduplicator.py', 'twitter_poster.py', 'main.py', 'requirements.txt', '.env']

    for file in required_files:
        if os.path.exists(file):
            print(f"   ✅ {file}")
            tests_passed += 1
        else:
            print(f"   ❌ {file}")
            tests_failed += 1

    # Test 2: Test news fetcher (works with basic dependencies)
    print("\n2. Testing news fetcher...")
    try:
        from news_fetcher import NewsFetcher
        fetcher = NewsFetcher()
        print("   ✅ News fetcher imported successfully")
        tests_passed += 1
    except Exception as e:
        print(f"   ❌ News fetcher failed: {e}")
        tests_failed += 1

    # Test 3: Test RSS feed fetching
    print("\n3. Testing RSS feed fetching...")
    try:
        from news_fetcher import NewsFetcher
        fetcher = NewsFetcher()
        articles = fetcher.fetch_from_rss('http://feeds.bbci.co.uk/news/rss.xml')
        print(f"   ✅ RSS test successful: {len(articles)} articles fetched")
        if articles:
            print(f"   📰 Sample: {articles[0]['title'][:50]}...")
        tests_passed += 1
    except Exception as e:
        print(f"   ❌ RSS test failed: {e}")
        tests_failed += 1

    # Test 4: Check .env configuration
    print("\n4. Checking environment configuration...")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        db_name = os.getenv('DB_NAME', 'not_set')
        if db_name != 'not_set':
            print(f"   ✅ Environment loaded: DB_NAME={db_name}")
            tests_passed += 1
        else:
            print("   ⚠️  Environment loaded but DB_NAME not set")
            tests_failed += 1
    except Exception as e:
        print(f"   ❌ Environment test failed: {e}")
        tests_failed += 1

    # Summary
    print(f"\n{'=' * 40}")
    print(f"📊 Test Results:")
    print(f"   ✅ Passed: {tests_passed}")
    print(f"   ❌ Failed: {tests_failed}")

    if tests_failed == 0:
        print(f"\n🎉 All basic tests passed!")
        print(f"   Your Free AI News Bot is working!")
        return True
    else:
        print(f"\n⚠️  {tests_failed} test(s) failed.")
        print(f"   Check the errors above for details.")
        return False

def show_next_steps():
    """Show what to do next"""
    print(f"\n🚀 Next Steps:")
    print(f"   1. Set up PostgreSQL database")
    print(f"   2. Configure .env with database credentials")
    print(f"   3. Install heavy dependencies: pip install transformers torch")
    print(f"   4. Run database migration: python migrate_database.py")
    print(f"   5. Start the bot: python run_bot.py")

if __name__ == "__main__":
    success = test_basic_functionality()
    show_next_steps()
    sys.exit(0 if success else 1)