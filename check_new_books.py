import asyncio
import sqlite3
import logging.config

import yaml
import numpy as np
import pandas as pd

from gsheet_utils import check_author
from scraper.master_of_spiders import command_spiders
from goodreads_api import check_goodreads
from form_final_books import form_final_books


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


async def main():
    '''Everything happens here.'''
    
    author = check_author() # get author's name from Google Sheet
    command_spiders(author) # run scrapy spiders
    await check_goodreads(author) # get Goodreads data
    await form_final_books(author) # match scraped data and data from Goodreads
      
    
if __name__ == '__main__':
    asyncio.run(main())