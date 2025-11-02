"""
Enhanced Deduplicator Module - Multi-layer Duplicate Detection
Goes beyond basic URL constraints to detect near-duplicates and semantic similarity
"""

import hashlib
import logging
import re
import psycopg2
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import os

# Try to import advanced deduplication libraries
try:
    import textdedup  # For MinHash, SimHash, SuffixArray
    TEXTPDEDUP_AVAILABLE = True
except ImportError:
    TEXTPDEDUP_AVAILABLE = False
    logging.warning("text-dedup not available - using basic deduplication only")

try:
    from Levenshtein import distance as levenshtein_distance
    LEVENSHTEIN_AVAILABLE = True
except ImportError:
    LEVENSHTEIN_AVAILABLE = False
    logging.warning("python-Levenshtein not available - using basic similarity only")


class EnhancedDeduplicator:
    def __init__(self, db_connection):
        self.logger = logging.getLogger(__name__)
        self.conn = db_connection
        self.similarity_cache = {}
        self.cache_expiry = timedelta(hours=24)

        # Similarity thresholds from planning.md
        self.title_similarity_threshold = 0.8  # 80% for title similarity
        self.semantic_similarity_threshold = 0.85  # 85% for semantic similarity

        # Initialize database schema if needed
        self._ensure_database_schema()

    def _ensure_database_schema(self):
        """Ensure database has the required columns for enhanced deduplication"""
        try:
            cursor = self.conn.cursor()

            # Add content_hash column if it doesn't exist
            cursor.execute("""
                ALTER TABLE articles
                ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
            """)

            # Add semantic_hash column if it doesn't exist
            cursor.execute("""
                ALTER TABLE articles
                ADD COLUMN IF NOT EXISTS semantic_hash VARCHAR(64);
            """)

            # Create indexes for performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_content_hash ON articles(content_hash);
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_semantic_hash ON articles(semantic_hash);
            """)

            # Create index on published_at for efficient recent article queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
            """)

            self.conn.commit()
            cursor.close()
            self.logger.info("Database schema updated for enhanced deduplication")

        except Exception as e:
            self.logger.error(f"Error updating database schema: {str(e)}")
            self.conn.rollback()

    def _normalize_content(self, content):
        """Normalize content for hash generation and comparison"""
        if not content:
            return ""

        # Convert to lowercase
        content = content.lower()

        # Remove HTML tags
        content = re.sub(r'<[^>]+>', '', content)

        # Remove URLs
        content = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', content)

        # Remove extra whitespace
        content = re.sub(r'\s+', ' ', content)

        # Remove punctuation and special characters (keep basic sentence structure)
        content = re.sub(r'[^\w\s\.\!\?\,\-]', '', content)

        # Standardize dates and numbers (basic approach)
        content = re.sub(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', '[DATE]', content)
        content = re.sub(r'\b\d{1,2}:\d{2}\s*(?:am|pm)?\b', '[TIME]', content)
        content = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', '[NUMBER]', content)

        return content.strip()

    def _generate_content_hash(self, content):
        """Generate SHA256 hash of normalized content for exact duplicate detection"""
        normalized_content = self._normalize_content(content)
        return hashlib.sha256(normalized_content.encode('utf-8')).hexdigest()

    def _generate_semantic_hash(self, content):
        """Generate semantic hash for near-duplicate detection"""
        try:
            normalized_content = self._normalize_content(content)

            if TEXTPDEDUP_AVAILABLE:
                # Use MinHash for semantic similarity
                # Note: This is a simplified implementation
                # In practice, you'd use textdedup's MinHash implementation
                words = normalized_content.split()[:100]  # Limit to first 100 words
                if len(words) < 5:
                    # Fallback to content hash for very short content
                    return self._generate_content_hash(content)

                # Simple semantic hash: take first and last few words + length
                semantic_content = f"{' '.join(words[:10])} {' '.join(words[-10:])} {len(words)}"
                return hashlib.sha256(semantic_content.encode('utf-8')).hexdigest()
            else:
                # Fallback: use a combination of content features
                words = normalized_content.split()
                if len(words) < 5:
                    return self._generate_content_hash(content)

                # Use word count, first/last words, and some middle words
                first_words = ' '.join(words[:5])
                last_words = ' '.join(words[-5:])
                middle_word = words[len(words)//2] if len(words) > 10 else ""

                semantic_content = f"{first_words} {middle_word} {last_words} {len(words)}"
                return hashlib.sha256(semantic_content.encode('utf-8')).hexdigest()

        except Exception as e:
            self.logger.warning(f"Error generating semantic hash: {str(e)}")
            return self._generate_content_hash(content)

    def _calculate_title_similarity(self, title1, title2):
        """Calculate similarity between two titles"""
        if not title1 or not title2:
            return 0.0

        try:
            # Normalize titles
            title1_norm = self._normalize_content(title1)
            title2_norm = self._normalize_content(title2)

            if LEVENSHTEIN_AVAILABLE:
                # Use Levenshtein distance for more accurate similarity
                max_len = max(len(title1_norm), len(title2_norm))
                if max_len == 0:
                    return 1.0

                distance = levenshtein_distance(title1_norm, title2_norm)
                similarity = 1 - (distance / max_len)
                return similarity

            else:
                # Fallback to SequenceMatcher
                return SequenceMatcher(None, title1_norm, title2_norm).ratio()

        except Exception as e:
            self.logger.warning(f"Error calculating title similarity: {str(e)}")
            return 0.0

    def _check_url_duplicate(self, url):
        """Check if URL already exists in database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM articles WHERE url = %s LIMIT 1", (url,))
            result = cursor.fetchone()
            cursor.close()
            return result is not None

        except Exception as e:
            self.logger.error(f"Error checking URL duplicate: {str(e)}")
            return False

    def _check_content_hash_duplicate(self, content_hash):
        """Check if content hash already exists"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id, title FROM articles
                WHERE content_hash = %s
                AND created_at > %s
                LIMIT 1
            """, (content_hash, datetime.now() - timedelta(days=30)))

            result = cursor.fetchone()
            cursor.close()
            return result

        except Exception as e:
            self.logger.error(f"Error checking content hash duplicate: {str(e)}")
            return None

    def _check_semantic_similarity(self, semantic_hash, title, content):
        """Check for semantically similar articles"""
        try:
            cursor = self.conn.cursor()

            # Find articles with similar semantic hash or recent titles
            cursor.execute("""
                SELECT id, title, semantic_hash, content_hash
                FROM articles
                WHERE created_at > %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (datetime.now() - timedelta(days=7),))

            recent_articles = cursor.fetchall()
            cursor.close()

            for article_id, existing_title, existing_semantic_hash, existing_content_hash in recent_articles:
                # Check semantic hash similarity (simple prefix matching)
                if semantic_hash and existing_semantic_hash:
                    # Compare first 16 characters of semantic hashes
                    if semantic_hash[:16] == existing_semantic_hash[:16]:
                        self.logger.debug(f"Semantic hash match found: {semantic_hash[:16]}")
                        return {
                            'duplicate': True,
                            'reason': 'semantic_similarity',
                            'existing_title': existing_title,
                            'article_id': article_id
                        }

                # Check title similarity
                title_similarity = self._calculate_title_similarity(title, existing_title)
                if title_similarity > self.title_similarity_threshold:
                    self.logger.debug(f"Title similarity: {title_similarity:.2f} > threshold")
                    return {
                        'duplicate': True,
                        'reason': 'title_similarity',
                        'similarity': title_similarity,
                        'existing_title': existing_title,
                        'article_id': article_id
                    }

            return {'duplicate': False}

        except Exception as e:
            self.logger.error(f"Error checking semantic similarity: {str(e)}")
            return {'duplicate': False}

    def check_duplicates(self, article_data):
        """
        Main method to check if an article is a duplicate
        Returns: {'is_duplicate': bool, 'reason': str, 'details': dict}
        """
        try:
            url = article_data.get('url', '')
            title = article_data.get('headline', '') or article_data.get('title', '')
            content = article_data.get('summary', '') or article_data.get('description', '')

            # Layer 1: URL Check (existing functionality)
            if self._check_url_duplicate(url):
                self.logger.debug(f"URL duplicate found: {url}")
                return {
                    'is_duplicate': True,
                    'reason': 'url_duplicate',
                    'details': {'url': url}
                }

            # Generate hashes
            content_hash = self._generate_content_hash(content)
            semantic_hash = self._generate_semantic_hash(content)

            # Layer 2: Content Hash Check (exact duplicates)
            content_duplicate = self._check_content_hash_duplicate(content_hash)
            if content_duplicate:
                self.logger.debug(f"Content hash duplicate found: {content_hash[:16]}...")
                return {
                    'is_duplicate': True,
                    'reason': 'content_duplicate',
                    'details': {
                        'content_hash': content_hash[:16] + '...',
                        'existing_title': content_duplicate[1]
                    }
                }

            # Layer 3: Semantic Similarity Check (near duplicates)
            semantic_result = self._check_semantic_similarity(semantic_hash, title, content)
            if semantic_result['duplicate']:
                return {
                    'is_duplicate': True,
                    'reason': semantic_result['reason'],
                    'details': semantic_result
                }

            # No duplicates found
            return {
                'is_duplicate': False,
                'reason': 'unique',
                'details': {
                    'content_hash': content_hash,
                    'semantic_hash': semantic_hash
                }
            }

        except Exception as e:
            self.logger.error(f"Error in duplicate checking: {str(e)}")
            # Fail safe: allow article through if deduplication fails
            return {
                'is_duplicate': False,
                'reason': 'error',
                'details': {'error': str(e)}
            }

    def get_article_with_hashes(self, article_data):
        """
        Add hash information to article data before database insertion
        """
        try:
            content = article_data.get('summary', '') or article_data.get('description', '')
            content_hash = self._generate_content_hash(content)
            semantic_hash = self._generate_semantic_hash(content)

            # Add hashes to article data
            enhanced_article = article_data.copy()
            enhanced_article['content_hash'] = content_hash
            enhanced_article['semantic_hash'] = semantic_hash

            return enhanced_article

        except Exception as e:
            self.logger.error(f"Error adding hashes to article: {str(e)}")
            return article_data

    def cleanup_old_hashes(self):
        """Clean up old semantic similarity cache data"""
        try:
            cursor = self.conn.cursor()

            # Remove old article references from semantic similarity checks
            # This helps maintain performance over time
            cutoff_date = datetime.now() - timedelta(days=30)

            cursor.execute("""
                UPDATE articles
                SET semantic_hash = NULL
                WHERE created_at < %s AND semantic_hash IS NOT NULL
            """, (cutoff_date,))

            deleted_count = cursor.rowcount
            self.conn.commit()
            cursor.close()

            self.logger.info(f"Cleaned up {deleted_count} old semantic hash entries")

        except Exception as e:
            self.logger.error(f"Error cleaning up old hashes: {str(e)}")
            self.conn.rollback()


def create_deduplicator(db_connection):
    """Factory function to create deduplicator instance"""
    return EnhancedDeduplicator(db_connection)


if __name__ == "__main__":
    # Test the deduplicator
    logging.basicConfig(level=logging.INFO)

    # You would need a real database connection to test this
    print("Deduplicator module loaded successfully")
    print("To test, provide a database connection and call check_duplicates()")