"""
Twitter Web Automation Module - Selenium-based Twitter Posting
Replaces Twitter API with web automation for free posting
"""

import os
import time
import logging
import random
import json
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    logging.warning("webdriver-manager not available - manual chromedriver required")

try:
    import undetected_chromedriver as uc
    UNDETECTED_CHROME_AVAILABLE = True
except ImportError:
    UNDETECTED_CHROME_AVAILABLE = False
    logging.warning("undetected-chromedriver not available - using standard selenium")


class TwitterWebPoster:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.driver = None
        self.session_path = os.getenv('TWITTER_SESSION_PATH', './twitter_session')
        self.max_tweets_per_day = int(os.getenv('MAX_TWEETS_PER_DAY', '10'))
        self.headless = os.getenv('TWITTER_HEADLESS', 'true').lower() == 'true'
        self.last_post_time = None
        self.posts_today = 0
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Create session directory if it doesn't exist
        os.makedirs(self.session_path, exist_ok=True)

        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        # Initialize browser
        self._initialize_browser()

    def _initialize_browser(self):
        """Initialize Chrome browser with anti-detection measures"""
        try:
            chrome_options = Options()

            # Basic options
            if self.headless:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-plugins')
            chrome_options.add_argument('--disable-images')  # Faster loading

            # Anti-detection options
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # User agent rotation
            chrome_options.add_argument(f'--user-agent={random.choice(self.user_agents)}')

            # Set up user data directory for session persistence
            user_data_dir = os.path.join(self.session_path, 'chrome_profile')
            chrome_options.add_argument(f'--user-data-dir={user_data_dir}')

            # Initialize driver
            if UNDETECTED_CHROME_AVAILABLE:
                self.logger.info("Using undetected-chromedriver for better anti-detection")
                self.driver = uc.Chrome(options=chrome_options, version_main=120)
            else:
                self.logger.info("Using standard Chrome driver")
                if WEBDRIVER_MANAGER_AVAILABLE:
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    # Fallback to system chromedriver
                    self.driver = webdriver.Chrome(options=chrome_options)

            # Set page load timeout
            self.driver.set_page_load_timeout(30)

            # Execute anti-detection scripts
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            self.logger.info("Browser initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize browser: {str(e)}")
            raise

    def _random_delay(self, min_seconds=2, max_seconds=5):
        """Add random delay to simulate human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        self.logger.debug(f"Random delay: {delay:.2f} seconds")
        time.sleep(delay)

    def _type_with_human_delay(self, element, text):
        """Type text with human-like delays"""
        element.clear()
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))  # Human typing speed

    def _check_rate_limits(self):
        """Check if we're within rate limits"""
        current_time = datetime.now()

        # Reset daily counter at midnight
        if current_time > self.daily_reset_time + timedelta(days=1):
            self.posts_today = 0
            self.daily_reset_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            self.logger.info("Daily tweet counter reset")

        # Check daily limit
        if self.posts_today >= self.max_tweets_per_day:
            self.logger.warning(f"Daily tweet limit reached: {self.posts_today}/{self.max_tweets_per_day}")
            return False

        # Check minimum delay between posts (30 minutes)
        if self.last_post_time:
            time_since_last_post = current_time - self.last_post_time
            if time_since_last_post < timedelta(minutes=30):
                remaining_time = timedelta(minutes=30) - time_since_last_post
                self.logger.warning(f"Rate limit: must wait {remaining_time}")
                return False

        # Check quiet hours (2 AM - 6 AM)
        if 2 <= current_time.hour <= 6:
            self.logger.info("Quiet hours - skipping posting")
            return False

        return True

    def _is_logged_in(self):
        """Check if we're logged into Twitter"""
        try:
            # Check for login indicators
            self.driver.get("https://twitter.com/home")
            time.sleep(3)

            # Look for indicators of being logged in
            login_indicators = [
                "//div[@data-testid='SideNav_AccountSwitcher_Button']",
                "//a[@aria-label='Home']",
                "//div[@data-testid='primaryColumn']"
            ]

            for indicator in login_indicators:
                try:
                    element = self.driver.find_element(By.XPATH, indicator)
                    if element:
                        self.logger.debug("Login indicator found")
                        return True
                except NoSuchElementException:
                    continue

            # Check if we're on login page
            try:
                login_element = self.driver.find_element(By.XPATH, "//input[@name='text' or @name='session[username_or_email]']")
                if login_element:
                    self.logger.debug("Login page detected - not logged in")
                    return False
            except NoSuchElementException:
                pass

            return True

        except Exception as e:
            self.logger.warning(f"Error checking login status: {str(e)}")
            return False

    def login_if_needed(self):
        """Check login status and guide manual login if needed"""
        try:
            if self._is_logged_in():
                self.logger.info("Already logged into Twitter")
                return True

            self.logger.warning("Not logged into Twitter - manual login required")
            self.logger.info("Please log in manually. The browser will wait.")

            # Go to Twitter login page
            self.driver.get("https://twitter.com/i/flow/login")
            time.sleep(5)

            if not self.headless:
                self.logger.info("Browser window opened. Please log in manually.")
                self.logger.info("Waiting for login completion...")

                # Wait for manual login (check every 10 seconds for up to 5 minutes)
                max_wait_time = 300  # 5 minutes
                wait_interval = 10
                elapsed_time = 0

                while elapsed_time < max_wait_time:
                    if self._is_logged_in():
                        self.logger.info("Login detected successfully!")
                        return True

                    time.sleep(wait_interval)
                    elapsed_time += wait_interval
                    self.logger.debug(f"Waiting for login... ({elapsed_time}s elapsed)")

            else:
                self.logger.error("Cannot perform manual login in headless mode")
                self.logger.info("Please run once without headless mode to login: TWITTER_HEADLESS=false")
                return False

            self.logger.error("Login timeout - please try again")
            return False

        except Exception as e:
            self.logger.error(f"Error during login process: {str(e)}")
            return False

    def _format_tweet_text(self, headline, url):
        """Format tweet text with character limit"""
        # Twitter character limit is 280
        tweet_text = f"{headline}\n\n{url}"

        # Truncate if too long
        if len(tweet_text) > 280:
            # Calculate available space for headline
            url_length = len(url) + 2  # +2 for newlines
            max_headline_length = 280 - url_length - 2  # -2 for extra newlines

            if max_headline_length > 10:
                headline = headline[:max_headline_length-3] + "..."
                tweet_text = f"{headline}\n\n{url}"

        return tweet_text

    def post_to_twitter(self, headline, url):
        """Main method to post a tweet - replaces post_tweet() from main.py"""
        try:
            # Check rate limits first
            if not self._check_rate_limits():
                return {
                    'success': False,
                    'reason': 'rate_limit',
                    'details': 'Rate limit or daily limit reached'
                }

            # Check if logged in
            if not self._is_logged_in():
                if not self.login_if_needed():
                    return {
                        'success': False,
                        'reason': 'login_required',
                        'details': 'Manual login required'
                    }

            # Format tweet text
            tweet_text = self._format_tweet_text(headline, url)
            self.logger.info(f"Posting tweet: {headline[:50]}...")

            # Navigate to Twitter home
            self.driver.get("https://twitter.com/home")
            self._random_delay(3, 6)

            # Find tweet compose area
            tweet_selectors = [
                "//div[@data-testid='tweetTextarea_0']",
                "//div[@aria-label='Tweet text']",
                "//div[@data-testid='tweetTextarea_0_label']//following-sibling::div",
                "//div[contains(@class, 'public-DraftEditorPlaceholder')]//following-sibling::div"
            ]

            tweet_box = None
            for selector in tweet_selectors:
                try:
                    tweet_box = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    if tweet_box:
                        break
                except TimeoutException:
                    continue

            if not tweet_box:
                # Try alternative approach - look for any contenteditable div
                try:
                    tweet_box = self.driver.find_element(By.CSS_SELECTOR, "div[contenteditable='true'][aria-label*='Tweet']")
                except NoSuchElementException:
                    pass

            if not tweet_box:
                self.logger.error("Could not find tweet compose area")
                return {
                    'success': False,
                    'reason': 'ui_error',
                    'details': 'Could not locate tweet compose area'
                }

            # Type the tweet with human-like behavior
            self._type_with_human_delay(tweet_box, tweet_text)
            self._random_delay(2, 4)

            # Find and click the tweet button
            tweet_button_selectors = [
                "//div[@data-testid='tweetButtonInline']",
                "//div[@data-testid='tweetButton']",
                "//button[@data-testid='tweetButton']",
                "//div[contains(@class, 'tweet-button')]//button"
            ]

            tweet_button = None
            for selector in tweet_button_selectors:
                try:
                    tweet_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    if tweet_button:
                        break
                except TimeoutException:
                    continue

            if not tweet_button:
                self.logger.error("Could not find tweet button")
                return {
                    'success': False,
                    'reason': 'ui_error',
                    'details': 'Could not locate tweet button'
                }

            # Click the tweet button
            self._random_delay(1, 3)
            tweet_button.click()

            # Wait for tweet to post
            self._random_delay(3, 6)

            # Verify tweet was posted
            try:
                # Look for success indicators
                success_indicators = [
                    "//div[contains(@aria-label, 'Your Tweet was sent')]",
                    "//div[contains(@text, 'Tweet sent')]",
                    "//div[@data-testid='toast']"
                ]

                for indicator in success_indicators:
                    try:
                        success_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, indicator))
                        )
                        if success_element:
                            self.logger.info("Tweet posted successfully!")
                            break
                    except TimeoutException:
                        continue
                else:
                    # No explicit success message, but assume success if no errors
                    self.logger.info("Tweet appears to have posted (no error detected)")

            except Exception as e:
                self.logger.warning(f"Could not verify tweet posting: {str(e)}")

            # Update tracking
            self.last_post_time = datetime.now()
            self.posts_today += 1

            return {
                'success': True,
                'reason': 'posted',
                'details': {
                    'headline': headline,
                    'url': url,
                    'posts_today': self.posts_today,
                    'timestamp': self.last_post_time.isoformat()
                }
            }

        except Exception as e:
            self.logger.error(f"Error posting tweet: {str(e)}")
            return {
                'success': False,
                'reason': 'error',
                'details': str(e)
            }

    def save_screenshot(self, filename_prefix="twitter_error"):
        """Save screenshot for debugging"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}.png"
            filepath = os.path.join(self.session_path, filename)

            if self.driver:
                self.driver.save_screenshot(filepath)
                self.logger.info(f"Screenshot saved: {filepath}")
                return filepath
        except Exception as e:
            self.logger.error(f"Error saving screenshot: {str(e)}")
        return None

    def cleanup(self):
        """Clean up browser resources"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                self.logger.info("Browser closed successfully")
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {str(e)}")


def post_to_twitter(headline, url):
    """Convenience function that matches the interface expected by main.py"""
    try:
        poster = TwitterWebPoster()
        result = poster.post_to_twitter(headline, url)
        poster.cleanup()
        return result['success']
    except Exception as e:
        logging.error(f"Error in post_to_twitter convenience function: {str(e)}")
        return False


if __name__ == "__main__":
    # Test the Twitter poster
    logging.basicConfig(level=logging.INFO)

    # Note: This requires manual login first
    print("Twitter Web Poster module loaded successfully")
    print("To test, set TWITTER_HEADLESS=false and run with manual login")
    print("Example usage:")
    print("  poster = TwitterWebPoster()")
    print("  result = poster.post_to_twitter('Test Headline', 'https://example.com')")
    print("  poster.cleanup()")