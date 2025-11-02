import os
import psycopg2
import time
import signal
import logging
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Import new free modules
from news_fetcher import fetch_from_all_sources
from summarizer import summarize_with_ai
from deduplicator import EnhancedDeduplicator
from twitter_poster import TwitterWebPoster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Global flag for controlling the bot
running = True

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logging.info("Shutdown signal received. Cleaning up...")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Load environment variables
load_dotenv()

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'turbotrends'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        logging.info("Database connected successfully!")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def fetch_news():
    """Fetch news articles using free RSS feeds and web scraping"""
    try:
        articles = fetch_from_all_sources()
        logging.info(f"Successfully fetched {len(articles)} articles from free sources")
        return articles
    except Exception as e:
        logging.error(f"Error fetching news: {str(e)}")
        return []

def summarize_article(article):
    """Use local AI models to summarize article and generate a catchy headline"""
    try:
        return summarize_with_ai(article)
    except Exception as e:
        logging.error(f"Error summarizing article: {str(e)}")
        return {
            'headline': article.get('title', 'No Title')[:120],
            'summary': article.get('description', '')[:200]
        }

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            summary TEXT,
            original_title TEXT,
            source TEXT,
            url TEXT UNIQUE,
            published_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            content_hash VARCHAR(64),
            semantic_hash VARCHAR(64)
        );
    """)
    conn.commit()
    cursor.close()
    logging.info("Database table 'articles' ready")

    # Initialize enhanced deduplication schema
    try:
        deduplicator = EnhancedDeduplicator(conn)
        logging.info("Enhanced deduplication schema initialized")
    except Exception as e:
        logging.warning(f"Could not initialize enhanced deduplication: {str(e)}")

def insert_article(conn, article_data, deduplicator):
    cursor = conn.cursor()
    try:
        # Enhanced deduplication check
        duplicate_check = deduplicator.check_duplicates(article_data)
        if duplicate_check['is_duplicate']:
            logging.debug(f"Duplicate article detected: {duplicate_check['reason']}")
            return False

        # Add hashes to article data
        enhanced_article = deduplicator.get_article_with_hashes(article_data)

        cursor.execute("""
            INSERT INTO articles (title, summary, original_title, source, url, published_at, content_hash, semantic_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            RETURNING id;
        """, (
            enhanced_article['headline'],
            enhanced_article['summary'],
            enhanced_article['original_title'],
            enhanced_article['source'],
            enhanced_article['url'],
            enhanced_article['published_at'],
            enhanced_article.get('content_hash'),
            enhanced_article.get('semantic_hash')
        ))
        result = cursor.fetchone()
        conn.commit()
        if result:
            logging.info(f"New article added: '{enhanced_article['headline']}'")
            return True
        else:
            logging.debug(f"Article already exists: '{enhanced_article['headline']}'")
            return False
    except Exception as e:
        logging.error(f"Error inserting article: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def post_tweet(twitter_poster, headline, url):
    """Post a tweet using web automation"""
    try:
        result = twitter_poster.post_to_twitter(headline, url)
        if result['success']:
            logging.info(f"Tweet posted successfully: {headline}")
            return True
        else:
            logging.warning(f"Tweet posting failed: {result['reason']} - {result['details']}")
            return False
    except Exception as e:
        logging.error(f"Error posting tweet: {str(e)}")
        return False

def init_twitter():
    """Initialize Twitter web automation client"""
    try:
        return TwitterWebPoster()
    except Exception as e:
        logging.error(f"Error initializing Twitter client: {str(e)}")
        return None

def process_article(article, conn, twitter_poster, deduplicator):
    """Process a single article through the pipeline"""
    # Generate summary and headline
    processed = summarize_article(article)

    article_data = {
        'headline': processed['headline'],
        'summary': processed['summary'],
        'original_title': article['title'],
        'source': article['source'],
        'url': article['link'],  # RSS feeds use 'link' instead of 'url'
        'published_at': article['published_at'] if isinstance(article['published_at'], datetime) else datetime.now()
    }

    # Insert into database with enhanced deduplication
    if insert_article(conn, article_data, deduplicator):
        # Post to Twitter only if it's a new article
        post_tweet(twitter_poster, article_data['headline'], article_data['url'])

def run_news_cycle(conn, twitter_poster, deduplicator):
    """Run one cycle of news fetching and processing"""
    try:
        articles = fetch_news()
        processed_count = 0

        for article in articles:
            if process_article(article, conn, twitter_poster, deduplicator):
                processed_count += 1

        logging.info(f"Cycle completed: {len(articles)} articles found, {processed_count} new articles processed")

    except Exception as e:
        logging.error(f"Error in news cycle: {str(e)}")
        # Reconnect to database if connection was lost
        if "connection" in str(e).lower():
            return connect_db()
    return conn

if __name__ == "__main__":
    # Initialize connections
    conn = connect_db()
    twitter_client = init_twitter()
    
    if not conn or not twitter_client:
        logging.error("Failed to initialize required connections. Exiting.")
        sys.exit(1)
    
    try:
        create_table(conn)
        
        # Configuration
        check_interval = int(os.getenv('CHECK_INTERVAL_MINUTES', '15'))  # Default to 15 minutes
        logging.info(f"Bot started. Checking for news every {check_interval} minutes")
        
        last_check = datetime.now() - timedelta(minutes=check_interval)  # Ensure first run happens immediately
        
        while running:
            current_time = datetime.now()
            
            # Check if it's time for the next cycle
            if (current_time - last_check).total_seconds() >= check_interval * 60:
                logging.info("Starting news check cycle...")
                conn = run_news_cycle(conn, twitter_client)
                last_check = current_time
            
            # Sleep for a short time to prevent CPU overuse
            time.sleep(60)  # Check every minute if it's time for the next cycle
            
    except Exception as e:
        logging.error(f"Critical error in main process: {str(e)}")
    finally:
        logging.info("Bot shutting down...")
        if conn:
            conn.close()
