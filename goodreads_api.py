import asyncio
import sqlite3
import logging.config
from typing import List, Any
from datetime import datetime
from pathlib import Path

import aiohttp
import xmltodict
import yaml
import pandas as pd

from credentials.credentials import GOODREADS_KEY


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


async def check_goodreads(author: str):
    '''Running requests within aiohttp session and writing results to database.'''
    
    URL = 'https://www.goodreads.com/author/list.xml'
    author_id = await get_author_id(author)
    
    async with aiohttp.ClientSession() as session:
        data = await get_books_data(session, URL, author_id)
        
        df = pd.DataFrame(data, columns=['book_id', 'book_title', 'title_without_series', 'publication_year', 'publication_month'])
        df[['publication_year', 'publication_month']] = df[['publication_year', 'publication_month']].fillna('Unknown')
        df['author_name'] = author
        df['author_id'] = author_id
        df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        with sqlite3.connect(Path(__file__).parent / 'db' / 'books.db') as conn:
            logger.info('Connected to database.')
            c = conn.cursor()
            # checking already existing in the database books by book_id
            c.execute('''SELECT book_id FROM goodreads_books
                         WHERE author_id = ?''', (author_id, )
            )
            existing_books = [str(i[0]) for i in c.fetchall()] # making ordinary list from list of tuples
            if len(existing_books) != 0:
                df = df.loc[~df['book_id'].isin(existing_books)] # removing already existing in the database books from the Dataframe
            if df['book_id'].size == 0:
                print('No new books from Goodreads.')
            else:
                print('New books from Goodreads:')
                for i in range(df['book_id'].size):
                    print(f'{df.iloc[i, 1]} by {df.iloc[i, 5]}. Publication date - {df.iloc[i, 4]}, {df.iloc[i, 3]}.')
                # writing only new books to DB
                df.to_sql('goodreads_books', conn, index=False, if_exists='append')
        conn.close()


async def get_author_id(author: str) -> str:
    '''Obtaining Goodreads author id.'''
    
    URL_PREFIX = 'https://www.goodreads.com/api/author_url/'
    url_modified = URL_PREFIX + author
    
    async with aiohttp.ClientSession() as session:
        payload = {
            'key': GOODREADS_KEY
        }
        async with session.get(url_modified, data=payload) as response:
            logger.info(f'Request status code for author id: {response.status}')        
            response_content = await response.text()
            author_id = xmltodict.parse(response_content)['GoodreadsResponse']['author']['@id']
            return author_id

    
async def get_books_data(session: aiohttp.ClientSession, url: str, 
                       author_id: str, page: int = 1) -> List[List[Any]]:
    '''Getting books data from Goodreads within aiohttp ClientSession.'''
    
    payload = {
        'key': GOODREADS_KEY,
        'id': author_id,
        'page': page
    }
    async with session.get(url, data=payload) as response:
        logger.info(f'Request status code for page {page}: {response.status}')
        response_content = await response.text()
        parsed_content = xmltodict.parse(response_content)['GoodreadsResponse']['author']['books']

        total_results = int(parsed_content['@total'])
        fetched_results = int(parsed_content['@end'])
        data = [
            [
                i['id']['#text'],
                i['title'],
                i.get('title_without_series', None),
                i.get('publication_year', None), 
                i.get('publication_month', None)
            ] for i in parsed_content['book']
        ]
        logger.debug(f'Fetched {fetched_results} out of {total_results}.')
        
        if fetched_results == total_results:
            return data
        # recursive part for fetching further pages if there are any
        elif fetched_results < total_results:
            await asyncio.sleep(1) # in order not to send more than 1 request per second
            return data + await get_books_data(session, url, author_id, page + 1)