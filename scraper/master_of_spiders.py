import importlib
import sqlite3
from pathlib import Path
import logging.config
from typing import List

import yaml
from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess

# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


# point scrapy project settings
settings = Settings()
settings_module_path = 'scraper.settings'
settings.setmodule(settings_module_path, priority='project')


def command_spiders(author: str):
    spider_class = check_fantasticfiction(author)
    crawl(spider_class)
    check_new_scraped_books()

    
def crawl(spider_class):
    # shutting down scrapy built-in logging
    logging.getLogger('scrapy').propagate = False
    
    process = CrawlerProcess(settings)
    process.crawl(spider_class)
    process.start() # the script will block here until the crawling is finished

    
def check_fantasticfiction(author: str):
    '''Check fantasticfiction table and import appropriate spider.'''
    with sqlite3.connect(Path(__file__).parent.parent / 'db' / 'books.db') as conn:
        logger.info('Connected to the database to check fantasticfiction.')
        c = conn.cursor()
        fantasticfiction_author = c.execute(
        '''SELECT author FROM fantasticfiction_authors
           WHERE author = ?''', (author, )
        ).fetchall()
        logger.debug(f'Retrieved fantasticfiction author: {fantasticfiction_author}')
        if len(fantasticfiction_author) == 0:
            logger.info('Fantasticfiction author not found.')
            spider_class = import_custom_class(author)
        else:
            if len(fantasticfiction_author) == 1:
                logger.info('Fantasticfiction author found.')
            else:
                logger.warning('Check fantasticfiction table in the database. You have duplicates.')
            # checking already existing books of an author in order not to crawl pages
            existing_books = c.execute(
            '''SELECT book_title FROM scraped_books
               WHERE author = ?''', (author, )
            ).fetchall()
            existing_books = [i[0] for i in existing_books] # making ordinary list from list of tuples
            spider_class = import_fantasticfiction_spider(author, existing_books)
    conn.close()
    logger.info('Database closed after checking fantasticfiction table.')
    return spider_class

    
def import_custom_class(author: str):
    '''Get specific class within specific module for a spider.'''
    module_name = author.replace(' ', '_').lower().strip()
    class_name = author.replace(' ', '').strip()
    spider_module = importlib.import_module('scraper.spiders.' + module_name)
    logger.info(f'Spider module imported: {spider_module}')
    spider_class = getattr(spider_module, class_name)
    logger.info(f'Spider class imported: {spider_class}')
    return spider_class


def import_fantasticfiction_spider(author: str, existing_books: List[str]):
    '''Import spider which runs on fantasticfiction.'''
    idx_letter = author.split()[-1][0].lower()
    author_page = author.replace(' ', '-').lower().strip()
    webpage = 'https://www.fantasticfiction.com/' + idx_letter + '/' + author_page + '/'
    logger.debug(f'Constructed webpage to crawl: {webpage}')
    spider_module = importlib.import_module('scraper.spiders.fantasticfiction')
    logger.info(f'Spider module imported: {spider_module}')
    spider_class = getattr(spider_module, 'FantasticFiction')
    logger.info(f'Spider class imported: {spider_class}')
    spider_class.author = author
    spider_class.start_urls = [webpage]
    spider_class.existing_books = existing_books
    return spider_class


def check_new_scraped_books():
    '''Check database if there are books added today and list them.'''
    with sqlite3.connect(Path(__file__).parent.parent / 'db' / 'books.db') as conn:
        logger.info('Connected to the database to check new scraped books.')
        c = conn.cursor()
        newly_scraped = c.execute('''
        SELECT book_title, author, series_title, publication_month, publication_year FROM scraped_books
        WHERE date(scraped_at) = date('now')
        ''').fetchall()
        if newly_scraped == []:
            print('No new books scraped.')
        else:
            print('Newly scraped books:')
            for i, j in list(enumerate(newly_scraped, 1)):
                print(f'{i}. {j[0]} by {j[1]}, publication date - {j[3]}, {j[4]}. {j[2]}.')
    conn.close()
    logger.info('Database connection closed.')