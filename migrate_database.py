"""
Database Migration Script - Enhanced Deduplication Schema
Adds columns and indexes for improved duplicate detection
"""

import os
import psycopg2
import logging
from datetime import datetime

def run_migration():
    """Run database migration for enhanced deduplication"""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'turbotrends'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )

        logger.info("Database connected successfully!")
        cursor = conn.cursor()

        # Migration steps
        migrations = [
            # Add content_hash column
            {
                'description': 'Add content_hash column',
                'sql': '''
                    ALTER TABLE articles
                    ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
                '''
            },

            # Add semantic_hash column
            {
                'description': 'Add semantic_hash column',
                'sql': '''
                    ALTER TABLE articles
                    ADD COLUMN IF NOT EXISTS semantic_hash VARCHAR(64);
                '''
            },

            # Create index on content_hash
            {
                'description': 'Create index on content_hash',
                'sql': '''
                    CREATE INDEX IF NOT EXISTS idx_content_hash ON articles(content_hash);
                '''
            },

            # Create index on semantic_hash
            {
                'description': 'Create index on semantic_hash',
                'sql': '''
                    CREATE INDEX IF NOT EXISTS idx_semantic_hash ON articles(semantic_hash);
                '''
            },

            # Create index on published_at for efficient recent article queries
            {
                'description': 'Create index on published_at',
                'sql': '''
                    CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
                '''
            },

            # Create index on created_at for cleanup operations
            {
                'description': 'Create index on created_at',
                'sql': '''
                    CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at);
                '''
            }
        ]

        # Run each migration
        for migration in migrations:
            logger.info(f"Running: {migration['description']}")
            try:
                cursor.execute(migration['sql'])
                logger.info(f"✓ {migration['description']} completed")
            except Exception as e:
                logger.error(f"✗ {migration['description']} failed: {str(e)}")
                # Continue with other migrations

        # Commit all changes
        conn.commit()

        # Verify the migration
        logger.info("Verifying migration...")
        cursor.execute('''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'articles'
            AND column_name IN ('content_hash', 'semantic_hash')
            ORDER BY column_name;
        ''')

        columns = cursor.fetchall()
        logger.info("Articles table columns after migration:")
        for column in columns:
            logger.info(f"  - {column[0]}: {column[1]}")

        # Check indexes
        cursor.execute('''
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE tablename = 'articles'
            AND indexname LIKE 'idx_%'
            ORDER BY indexname;
        ''')

        indexes = cursor.fetchall()
        logger.info("Indexes on articles table:")
        for index in indexes:
            logger.info(f"  - {index[0]}")

        # Get current article count
        cursor.execute("SELECT COUNT(*) FROM articles;")
        article_count = cursor.fetchone()[0]
        logger.info(f"Current article count: {article_count}")

        cursor.close()
        conn.close()

        logger.info("✓ Database migration completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False


if __name__ == "__main__":
    print("Running database migration for enhanced deduplication...")
    success = run_migration()

    if success:
        print("\n✓ Migration completed successfully!")
        print("Your database is now ready for enhanced deduplication.")
    else:
        print("\n✗ Migration failed!")
        print("Please check the error messages above and fix any issues.")
        print("You may need to check your database connection or permissions.")