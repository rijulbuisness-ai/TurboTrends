#!/usr/bin/env python3
"""
Test Twitter posting functionality
This will test if the Twitter automation actually works
"""

import os
import sys

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_twitter_functionality():
    """Test Twitter posting functionality"""
    print("🐦 Testing Twitter Posting Functionality")
    print("=" * 45)

    try:
        from twitter_poster import TwitterWebPoster
        print("✅ Twitter poster module imported successfully")

        # Initialize Twitter poster
        poster = TwitterWebPoster()
        print("✅ Twitter poster initialized successfully")

        # Test login status
        is_logged_in = poster._is_logged_in()
        print(f"📊 Login status: {'✅ Logged in' if is_logged_in else '❌ Not logged in'}")

        if not is_logged_in:
            print("\n🔧 Setup Required:")
            print("1. Set TWITTER_HEADLESS=false in .env file")
            print("2. Run: python test_twitter_setup.py")
            print("3. Login manually when browser opens")
            print("4. After login, set TWITTER_HEADLESS=true")
            return False

        # Test posting (without actually posting)
        print("\n🧪 Testing tweet formatting...")
        test_headline = "Test: Free AI News Bot is Working!"
        test_url = "https://github.com/example"

        # Test tweet text formatting
        tweet_text = poster._format_tweet_text(test_headline, test_url)
        print(f"📝 Formatted tweet: {len(tweet_text)} characters")
        print(f"   Content: {tweet_text[:100]}...")

        if len(tweet_text) <= 280:
            print("✅ Tweet formatting successful")
        else:
            print("❌ Tweet too long")
            return False

        # Test rate limiting
        print("\n⏰ Testing rate limiting...")
        can_post = poster._check_rate_limits()
        print(f"📊 Rate limit check: {'✅ Can post' if can_post else '❌ Rate limited'}")

        # Cleanup
        poster.cleanup()
        print("✅ Cleanup successful")

        return True

    except Exception as e:
        print(f"❌ Twitter test failed: {e}")
        return False

def test_actual_posting():
    """Test actual posting (optional - requires manual confirmation)"""
    print("\n🚀 ACTUAL POSTING TEST")
    print("=" * 30)
    print("⚠️  WARNING: This will post a real tweet to your Twitter account!")

    response = input("Do you want to test actual posting? (yes/no): ").lower().strip()

    if response != 'yes':
        print("❌ Actual posting test skipped")
        return False

    try:
        from twitter_poster import TwitterWebPoster

        poster = TwitterWebPoster()

        # Test tweet
        test_headline = "🤖 Free AI News Bot Test - Do Not Reply"
        test_url = "https://github.com/free-ai-news-bot"

        print(f"📤 Posting test tweet...")
        result = poster.post_to_twitter(test_headline, test_url)

        if result['success']:
            print("✅ TEST TWEET POSTED SUCCESSFULLY!")
            print(f"   Details: {result.get('details', {})}")
            return True
        else:
            print(f"❌ Tweet posting failed: {result['reason']}")
            print(f"   Details: {result['details']}")
            return False

    except Exception as e:
        print(f"❌ Actual posting test failed: {e}")
        return False
    finally:
        try:
            poster.cleanup()
        except:
            pass

if __name__ == "__main__":
    print("🐦 Twitter Functionality Test")
    print("=" * 50)

    # Test basic functionality
    basic_test_passed = test_twitter_functionality()

    if basic_test_passed:
        print("\n✅ Basic Twitter functionality test PASSED!")

        # Ask if user wants to test actual posting
        actual_test_passed = test_actual_posting()

        if actual_test_passed:
            print("\n🎉 TWITTER POSTING IS WORKING!")
            print("Your bot will be able to post tweets automatically!")
        else:
            print("\n⚠️  Twitter posting needs setup")
    else:
        print("\n❌ Basic Twitter functionality test FAILED")
        print("Check the errors above and fix the issues")