"""
News Fetcher Module - Free RSS and Web Scraping News Aggregation
Replaces News API with free alternatives: RSS feeds and web scraping
"""

import feedparser
import requests
import time
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
from urllib.parse import urljoin, urlparse
import random


class NewsFetcher:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()

        # Default RSS feeds from planning.md
        self.default_rss_feeds = [
            'http://feeds.bbci.co.uk/news/rss.xml',
            'http://rss.cnn.com/rss/edition.rss',
            'https://www.reuters.com/rssFeed/worldNews',
            'https://www.theguardian.com/world/rss',
            'https://feeds.npr.org/1001/rss.xml'
        ]

        # User agents for rotation to avoid blocking
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]

        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents)
        })

        # Rate limiting: 1 request per 2 seconds per domain
        self.last_request_time = {}
        self.min_delay = 2  # seconds

    def _rate_limit(self, domain):
        """Implement rate limiting to avoid being blocked"""
        current_time = time.time()
        if domain in self.last_request_time:
            elapsed = current_time - self.last_request_time[domain]
            if elapsed < self.min_delay:
                sleep_time = self.min_delay - elapsed
                self.logger.debug(f"Rate limiting: waiting {sleep_time:.2f}s for {domain}")
                time.sleep(sleep_time)

        self.last_request_time[domain] = time.time()

    def _get_domain(self, url):
        """Extract domain from URL for rate limiting"""
        return urlparse(url).netloc

    def fetch_from_rss(self, rss_url):
        """Fetch articles from a single RSS feed"""
        articles = []
        try:
            self._rate_limit(self._get_domain(rss_url))

            self.logger.info(f"Fetching RSS feed: {rss_url}")
            feed = feedparser.parse(rss_url)

            if feed.bozo:
                self.logger.warning(f"RSS feed parsing warning for {rss_url}: {feed.bozo_exception}")

            for entry in feed.entries:
                # Extract article data
                article = {
                    'title': entry.get('title', '').strip(),
                    'description': entry.get('description', ''),
                    'summary': entry.get('summary', ''),
                    'link': entry.get('link', ''),
                    'source': feed.feed.get('title', 'Unknown Source'),
                    'published_at': None,
                    'raw_content': entry.get('summary', '') or entry.get('description', '')
                }

                # Parse publication date
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        article['published_at'] = datetime(*entry.published_parsed[:6])
                    except (TypeError, ValueError):
                        self.logger.warning(f"Could not parse date for article: {article['title']}")
                        article['published_at'] = datetime.now()
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    try:
                        article['published_at'] = datetime(*entry.updated_parsed[:6])
                    except (TypeError, ValueError):
                        article['published_at'] = datetime.now()
                else:
                    article['published_at'] = datetime.now()

                # Filter out articles older than 24 hours
                if article['published_at'] < datetime.now() - timedelta(hours=24):
                    continue

                # Validate required fields
                if article['title'] and article['link']:
                    # Clean up content - remove HTML tags
                    article['raw_content'] = self._clean_html(article['raw_content'])
                    articles.append(article)
                    self.logger.debug(f"Added RSS article: {article['title']}")

        except Exception as e:
            self.logger.error(f"Error fetching RSS feed {rss_url}: {str(e)}")

        return articles

    def _clean_html(self, html_content):
        """Remove HTML tags from content"""
        if not html_content:
            return ""

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.get_text(separator=' ', strip=True)
        except Exception as e:
            self.logger.warning(f"Error cleaning HTML: {str(e)}")
            return html_content

    def scrape_article_content(self, url):
        """Scrape full content from an article URL (optional enhancement)"""
        try:
            self._rate_limit(self._get_domain(url))

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Try to find main content areas (common selectors)
            content_selectors = [
                'article',
                '.article-content',
                '.post-content',
                '.entry-content',
                '.content',
                'main',
                '.main-content'
            ]

            content = ""
            for selector in content_selectors:
                element = soup.select_one(selector)
                if element:
                    content = element.get_text(separator=' ', strip=True)
                    break

            # Fallback to body if no specific content found
            if not content and soup.body:
                content = soup.body.get_text(separator=' ', strip=True)

            # Limit content length for processing
            if len(content) > 2000:
                content = content[:2000] + "..."

            return content.strip()

        except Exception as e:
            self.logger.warning(f"Error scraping article content from {url}: {str(e)}")
            return ""

    def fetch_from_all_sources(self):
        """Main method to fetch news from all configured sources"""
        all_articles = []

        # Get RSS feeds from environment or use defaults
        rss_feeds_str = os.getenv('RSS_FEEDS_URLS', '')
        if rss_feeds_str:
            rss_feeds = [feed.strip() for feed in rss_feeds_str.split(',') if feed.strip()]
        else:
            rss_feeds = self.default_rss_feeds

        self.logger.info(f"Fetching news from {len(rss_feeds)} RSS feeds")

        # Fetch from RSS feeds
        for rss_url in rss_feeds:
            try:
                articles = self.fetch_from_rss(rss_url)
                all_articles.extend(articles)
                self.logger.info(f"RSS {rss_url}: {len(articles)} articles fetched")

                # Small delay between feeds
                time.sleep(1)

            except Exception as e:
                self.logger.error(f"Failed to process RSS feed {rss_url}: {str(e)}")
                continue

        # Optional: Web scraping for sites without RSS feeds
        if os.getenv('ENABLE_WEB_SCRAPING', 'false').lower() == 'true':
            web_scrape_articles = self.fetch_from_web_scraping()
            all_articles.extend(web_scrape_articles)

        # Remove duplicates based on URL (basic deduplication)
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            if article['link'] not in seen_urls:
                seen_urls.add(article['link'])
                unique_articles.append(article)

        self.logger.info(f"Total unique articles fetched: {len(unique_articles)}")
        return unique_articles

    def fetch_from_web_scraping(self):
        """Basic web scraping for sites without RSS feeds"""
        articles = []

        # Get sites to scrape from environment
        sites_str = os.getenv('WEB_SCRAPE_SITES', '')
        if not sites_str:
            return articles

        sites = [site.strip() for site in sites_str.split(',') if site.strip()]

        self.logger.info(f"Web scraping {len(sites)} sites (if implemented)")

        # Note: Web scraping implementation would go here
        # This is a placeholder for sites that don't have RSS feeds
        # Implementation would depend on specific site structures

        return articles


def fetch_from_all_sources():
    """Convenience function that matches the interface expected by main.py"""
    fetcher = NewsFetcher()
    return fetcher.fetch_from_all_sources()


if __name__ == "__main__":
    # Test the news fetcher
    logging.basicConfig(level=logging.INFO)

    fetcher = NewsFetcher()
    articles = fetcher.fetch_from_all_sources()

    print(f"Fetched {len(articles)} articles")
    for i, article in enumerate(articles[:3]):  # Show first 3
        print(f"\n{i+1}. {article['title']}")
        print(f"   Source: {article['source']}")
        print(f"   URL: {article['link']}")
        print(f"   Published: {article['published_at']}")
        print(f"   Description: {article['description'][:100]}...")