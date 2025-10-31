import os
import requests
import openai
import psycopg2
import tweepy
import time
import signal
import logging
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

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
    """Fetch news articles from News API"""
    api_key = os.getenv('NEWS_API_KEY')
    url = 'https://newsapi.org/v2/top-headlines'
    
    params = {
        'apiKey': api_key,
        'language': 'en',
        'pageSize': 10  # Fetch 10 articles at a time
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        articles = response.json()['articles']
        logging.info(f"Successfully fetched {len(articles)} articles from News API")
        return articles
    except Exception as e:
        logging.error(f"Error fetching news: {str(e)}")
        return []

def summarize_article(article):
    """Use OpenAI to summarize article and generate a catchy headline"""
    openai.api_key = os.getenv('OPENAI_API_KEY')
    
    # Combine title and content for context
    content = f"Title: {article['title']}\n\nContent: {article['description'] or article['content'] or ''}"
    
    try:
        # Generate summary
        summary_response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a news editor who creates concise summaries and engaging headlines."},
                {"role": "user", "content": f"Please summarize this news article in 2-3 sentences and create a catchy headline under 120 characters:\n\n{content}"}
            ],
            temperature=0.7,
            max_tokens=150
        )
        
        summary_text = summary_response.choices[0].message.content.strip()
        
        # Split the response into headline and summary
        parts = summary_text.split('\n\n', 1)
        headline = parts[0].replace('Headline: ', '')
        summary = parts[1] if len(parts) > 1 else headline
        
        return {
            'headline': headline,
            'summary': summary
        }
    except Exception as e:
        print(f"❌ Error summarizing article: {str(e)}")
        return {
            'headline': article['title'],
            'summary': article['description'] or ''
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    logging.info("Database table 'articles' ready")

def insert_article(conn, article_data):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO articles (title, summary, original_title, source, url, published_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            RETURNING id;
        """, (
            article_data['headline'],
            article_data['summary'],
            article_data['original_title'],
            article_data['source'],
            article_data['url'],
            article_data['published_at']
        ))
        result = cursor.fetchone()
        conn.commit()
        if result:
            logging.info(f"New article added: '{article_data['headline']}'")
            return True
        else:
            logging.debug(f"Article already exists: '{article_data['headline']}'")
            return False
    except Exception as e:
        logging.error(f"Error inserting article: {str(e)}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def post_tweet(client, headline, url):
    """Post a tweet with the headline and URL"""
    try:
        tweet_text = f"{headline}\n\n{url}"
        client.update_status(status=tweet_text)
        logging.info(f"Tweet posted successfully: {headline}")
        return True
    except Exception as e:
        logging.error(f"Error posting tweet: {str(e)}")
        return False

def init_twitter():
    """Initialize Twitter API client"""
    try:
        auth = tweepy.OAuthHandler(
            os.getenv('TWITTER_API_KEY'),
            os.getenv('TWITTER_API_SECRET')
        )
        auth.set_access_token(
            os.getenv('TWITTER_ACCESS_TOKEN'),
            os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        )
        return tweepy.API(auth)
    except Exception as e:
        print(f"❌ Error initializing Twitter client: {str(e)}")
        return None

def process_article(article, conn, twitter_client):
    """Process a single article through the pipeline"""
    # Generate summary and headline
    processed = summarize_article(article)
    
    article_data = {
        'headline': processed['headline'],
        'summary': processed['summary'],
        'original_title': article['title'],
        'source': article['source']['name'],
        'url': article['url'],
        'published_at': datetime.strptime(article['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
    }
    
    # Insert into database
    if insert_article(conn, article_data):
        # Post to Twitter only if it's a new article
        post_tweet(twitter_client, article_data['headline'], article_data['url'])

def run_news_cycle(conn, twitter_client):
    """Run one cycle of news fetching and processing"""
    try:
        articles = fetch_news()
        processed_count = 0
        
        for article in articles:
            if process_article(article, conn, twitter_client):
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
