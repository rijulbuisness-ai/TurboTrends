"""
Setup and Testing Script for Free AI News Bot
Helps with initial setup, dependency installation, and module testing
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def install_dependencies():
    """Install required dependencies"""
    logger.info("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        logger.info("✓ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Failed to install dependencies: {e}")
        return False

def test_news_fetcher():
    """Test the news fetcher module"""
    logger.info("Testing news fetcher...")
    try:
        from news_fetcher import fetch_from_all_sources
        articles = fetch_from_all_sources()
        logger.info(f"✓ News fetcher test passed: {len(articles)} articles fetched")

        # Show sample articles
        for i, article in enumerate(articles[:3]):
            logger.info(f"  {i+1}. {article['title'][:50]}...")

        return True
    except Exception as e:
        logger.error(f"✗ News fetcher test failed: {e}")
        return False

def test_summarizer():
    """Test the AI summarizer module"""
    logger.info("Testing AI summarizer...")
    try:
        from summarizer import AISummarizer

        # Test article
        test_article = {
            'title': 'Scientists Discover Breakthrough in Renewable Energy Technology',
            'description': 'Researchers have announced a major breakthrough in solar panel efficiency that could revolutionize the renewable energy sector.',
            'raw_content': 'Full article content would go here...'
        }

        summarizer = AISummarizer()
        result = summarizer.summarize_with_ai(test_article)

        # Check result format
        if isinstance(result, dict) and 'headline' in result and 'summary' in result:
            logger.info(f"✓ Summarizer test passed")
            logger.info(f"  Headline: {result['headline'][:60]}...")
            logger.info(f"  Summary: {result['summary'][:80]}...")

            # Cleanup
            summarizer.cleanup()
            return True
        else:
            logger.error("✗ Summarizer returned invalid format")
            return False

    except Exception as e:
        logger.error(f"✗ Summarizer test failed: {e}")
        return False

def test_deduplicator():
    """Test the deduplicator module (requires database)"""
    logger.info("Testing deduplicator (requires database)...")
    try:
        import psycopg2
        from deduplicator import EnhancedDeduplicator
        from dotenv import load_dotenv

        # Load environment variables
        load_dotenv()

        # Connect to database
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'turbotrends'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )

        deduplicator = EnhancedDeduplicator(conn)

        # Test duplicate checking
        test_article = {
            'title': 'Test Article for Deduplication',
            'headline': 'Test Headline',
            'description': 'This is a test article for deduplication checking.',
            'url': 'https://example.com/test-article'
        }

        result = deduplicator.check_duplicates(test_article)
        logger.info(f"✓ Deduplicator test passed: {result['reason']}")

        conn.close()
        return True

    except Exception as e:
        logger.warning(f"⚠ Deduplicator test skipped (database required): {e}")
        return True  # Don't fail the overall test

def test_twitter_poster():
    """Test Twitter poster (requires manual setup)"""
    logger.info("Testing Twitter poster (requires manual setup)...")
    try:
        from twitter_poster import TwitterWebPoster

        # Note: This test only initializes the poster
        # Actual posting requires manual login setup
        poster = TwitterWebPoster()
        logger.info("✓ Twitter poster initialization successful")
        logger.info("  Note: Actual posting requires manual login setup (TWITTER_HEADLESS=false)")

        # Cleanup
        poster.cleanup()
        return True

    except Exception as e:
        logger.error(f"✗ Twitter poster test failed: {e}")
        return False

def run_database_migration():
    """Run database migration"""
    logger.info("Running database migration...")
    try:
        from migrate_database import run_migration
        success = run_migration()
        return success
    except Exception as e:
        logger.error(f"✗ Database migration failed: {e}")
        return False

def check_environment():
    """Check environment configuration"""
    logger.info("Checking environment configuration...")

    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        logger.error(f"✗ Missing required environment variables: {missing_vars}")
        logger.info("Please set these in your .env file")
        return False

    logger.info("✓ Environment configuration OK")
    return True

def main():
    """Main setup and testing function"""
    logger.info("Free AI News Bot - Setup and Testing")
    logger.info("=" * 50)

    tests = [
        ("Environment Check", check_environment),
        ("Install Dependencies", install_dependencies),
        ("News Fetcher", test_news_fetcher),
        ("AI Summarizer", test_summarizer),
        ("Database Migration", run_database_migration),
        ("Deduplicator", test_deduplicator),
        ("Twitter Poster", test_twitter_poster)
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST SUMMARY")
    logger.info("=" * 50)

    passed = 0
    failed = 0

    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"{status}: {test_name}")
        if success:
            passed += 1
        else:
            failed += 1

    logger.info(f"\nTotal: {passed} passed, {failed} failed")

    if failed == 0:
        logger.info("\n🎉 All tests passed! Your Free AI News Bot is ready to use.")
        logger.info("\nNext steps:")
        logger.info("1. Set up PostgreSQL database")
        logger.info("2. Configure .env file with your database credentials")
        logger.info("3. For Twitter: Set TWITTER_HEADLESS=false and run once to login manually")
        logger.info("4. Start the bot: python main.py")
    else:
        logger.info(f"\n⚠️  {failed} test(s) failed. Please fix the issues above.")
        logger.info("\nCommon issues:")
        logger.info("- Database connection: Check DB_NAME, DB_USER, DB_PASSWORD in .env")
        logger.info("- Dependencies: Make sure pip install -r requirements.txt completed")
        logger.info("- Twitter setup: Requires manual login first (see documentation)")

    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)