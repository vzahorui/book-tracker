import importlib
import sqlite3
from pathlib import Path
import logging.config

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
    spider_class = import_spider_class(author)
    crawl(spider_class)
    check_new_scraped_books()

    
def crawl(spider_class):
    # shutting down scrapy built-in logging
    logging.getLogger('scrapy').propagate = False
    
    process = CrawlerProcess(settings)
    process.crawl(spider_class)
    process.start() # the script will block here until the crawling is finished

    
def import_spider_class(author: str):
    '''Get specific class within specific module for a spider.'''
    
    module_name = author.replace(' ', '_').lower().strip()
    class_name = author.replace(' ', '').strip()
    spider_module = importlib.import_module('scraper.spiders.' + module_name)
    spider_class = getattr(spider_module, class_name)
    logger.info('Spider class imported.')
    return spider_class  


def check_new_scraped_books():
    '''Check database if there are books added today and list them.'''
    
    with sqlite3.connect(Path(__file__).parent.parent / 'db' / 'books.db') as conn:
        logger.info('Connected to the database to check new scraped books.')
        c = conn.cursor()
        newly_scraped = c.execute('''
        SELECT book_title, author, series_title, pub_date FROM scraped_books
        WHERE date(scraped_at) = date('now')
        ''').fetchall()
        if newly_scraped == []:
            print('No new books scraped.')
        else:
            print('Newly scraped books:')
            for i, j in list(enumerate(newly_scraped, 1)):
                print(f'{i}. {j[0]} by {j[1]}, publication date - {j[3]}. {j[2]}.')
    conn.close()
    logger.debug('Database connection closed.')