"""
Basic Module Testing Script
Tests modules that don't require heavy dependencies
"""

import os
import sys

# Add current directory to Python path
sys.path.insert(0, '.')

def test_news_fetcher():
    """Test the news fetcher module"""
    print("Testing News Fetcher...")
    try:
        from news_fetcher import NewsFetcher, fetch_from_all_sources
        print("✓ News fetcher imported successfully")

        # Test instantiation
        fetcher = NewsFetcher()
        print("✓ News fetcher instantiated successfully")

        # Test with a simple RSS feed
        print("Testing RSS feed fetching...")
        articles = fetcher.fetch_from_rss('http://feeds.bbci.co.uk/news/rss.xml')
        print(f"✓ RSS feed test: {len(articles)} articles fetched")

        if articles:
            print(f"  Sample: {articles[0]['title'][:50]}...")

        return True
    except Exception as e:
        print(f"✗ News fetcher test failed: {e}")
        return False

def test_basic_imports():
    """Test basic imports without heavy dependencies"""
    print("Testing Basic Imports...")
    try:
        # Test modules that should work with basic dependencies
        import feedparser
        import requests
        import bs4
        from dotenv import load_dotenv
        print("✓ Basic dependencies available")

        # Test environment loading
        load_dotenv()
        print("✓ Environment loading works")

        return True
    except Exception as e:
        print(f"✗ Basic import test failed: {e}")
        return False

def test_module_structure():
    """Test that all module files exist and have expected structure"""
    print("Testing Module Structure...")
    modules_to_check = [
        'news_fetcher.py',
        'summarizer.py',
        'deduplicator.py',
        'twitter_poster.py',
        'main.py',
        'requirements.txt',
        '.env'
    ]

    for module in modules_to_check:
        if os.path.exists(module):
            print(f"✓ {module} exists")
        else:
            print(f"✗ {module} missing")
            return False

    return True

def main():
    """Run basic tests"""
    print("Free AI News Bot - Basic Module Testing")
    print("=" * 50)

    tests = [
        ("Basic Imports", test_basic_imports),
        ("Module Structure", test_module_structure),
        ("News Fetcher", test_news_fetcher),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"Test {test_name} crashed: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 50)
    print("BASIC TEST SUMMARY")
    print("=" * 50)

    passed = 0
    failed = 0

    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {test_name}")
        if success:
            passed += 1
        else:
            failed += 1

    print(f"\nTotal: {passed} passed, {failed} failed")

    if failed == 0:
        print("\n🎉 Basic tests passed!")
        print("\nFor full functionality, install remaining dependencies:")
        print("  pip install -r requirements.txt")
        print("\nFor AI summarization, install:")
        print("  pip install transformers torch sentencepiece")
        print("\nFor database support, install:")
        print("  pip install psycopg2-binary")
        print("\nFor Twitter automation, install:")
        print("  pip install selenium webdriver-manager undetected-chromedriver")
    else:
        print(f"\n⚠️  {failed} basic test(s) failed.")

    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)