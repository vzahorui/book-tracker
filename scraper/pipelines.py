import logging.config
from pathlib import Path
import sqlite3

import yaml


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


class BooksPipeline(object):
    
    def __init__(self):
        self.create_connection()
    
    def create_connection(self):
        db_path = Path(__file__).parent.parent / 'db' / 'books.db'
        self.conn = sqlite3.connect(db_path)
        logger.info('Database connection created.')
        self.cursor = self.conn.cursor()
    
    def process_item(self, item, spider):
        self.store_db(item)
        return item
    
    def store_db(self, item):
        self.cursor.execute('''
        INSERT INTO scraped_books_stage
        VALUES (?, ?, ?, ?, ?, ?, ?)''', (
            item['author'],
            item['book_title'],
            item['series_title'],
            item['number_in_series'],
            item['publication_year'],
            item['publication_month'],
            item['scraped_at']
        ))
        self.conn.commit()
        logger.debug('Inserted items into scraped_books_stage table: %s, %s, %s, %s,  %s,  %s, %s',
                     item['author'], item['book_title'], item['series_title'], item['number_in_series'],
                     item['publication_year'], item['publication_month'], item['scraped_at']
                    )
    
    def close_spider(self, spider):
        # add new books to scraped_books table and clear table scraped_books_stage
        self.cursor.executescript('''
        INSERT INTO scraped_books
        ('author', 'book_title', 'series_title', 'number_in_series',
         'publication_year', 'publication_month', 'scraped_at')
        SELECT ss.* FROM scraped_books_stage ss
        LEFT JOIN scraped_books s 
        ON s.author = ss.author and s.book_title = ss.book_title
        WHERE s.author IS NULL;
        
        DELETE FROM scraped_books_stage
        ''')
        logger.info('Checked new items for scraped_books table.')
        self.conn.commit()
        self.conn.close()
        logger.info('Database connection closed.')