import os
import asyncio
import sqlite3
from typing import List, Any

import aiohttp
import xmltodict
import numpy as np
import pandas as pd

from credentials.credentials import GOODREADS_KEY


async def get_response(session: aiohttp.ClientSession, url: str, 
                       author: str = 'Brent Weeks', page: int = 1) -> List[List[Any]]:
    '''
    Constuction for passing into other function for getting books list of a given author from Goodreads API. 
    The purpose of this function is to ensure recursive requests from a single aiohttp session.
    
    Parameters
    ----------
    session
        Session object created by ClientSession class of aiohttp.
    url
        Url for sending requests.
    author
        Author's name.
    page
        Which page of search results to return.
    
    Returns
    -------
    list
        List of lists containing values of iterest per search result item.
    
    '''
    payload = {
        'key': GOODREADS_KEY,
        'q': author,
        'search': 'author',
        'page': page
    }
    
    async with session.get(url, data=payload) as response:
        print(f'Request status code for page {page}:', response.status) # TODO handle various response statuses
        
        response_content = await response.text()
        parsed_content = xmltodict.parse(response_content)['GoodreadsResponse']['search']
        
        if parsed_content['results'] == None:
            print('Restarting request...')
            await asyncio.sleep(1) # in order not to send more than 1 request per second
            return await get_response(session, url, author, page)
        else:
            # list comprehension with additional filtering by author's name
            data = [
                     [i['best_book']['id'].get('#text', None),
                      i['best_book']['title'],
                      i['original_publication_year'].get('#text', None), 
                      i['original_publication_month'].get('#text', None),
                      i['best_book']['author']['id'].get('#text', None),
                      i['best_book']['author']['name']
                     ] for i in parsed_content['results']['work'] if i['best_book']['author']['name'] == author
                   ]
        
        total_results = int(parsed_content['total-results'])
        fetched_results = int(parsed_content['results-end'])
        
        print(f'Fetched {fetched_results} out of {total_results}.')
        if fetched_results == total_results:
            return data
        # recursive part
        elif fetched_results < total_results:
            retrieved_data = data.copy()
            await asyncio.sleep(1) # in order not to send more than 1 request per second
            return retrieved_data + await get_response(session, url, author, page + 1)


async def main():
    '''Creates aiohttp session and runs request function. Then writes results to SQLite DB.'''
    
    url = 'https://www.goodreads.com/search/index.xml'
    async with aiohttp.ClientSession() as session:
        data = await get_response(session, url) ## TODO run as tasks instea of coroutines (asyncio.create_task())
                    
        df = pd.DataFrame(data, columns=['book_id', 'book_title', 'publication_year', 'publication_month', 'author_id', 'author_name'])
        
        conn = sqlite3.connect(os.path.join('db', 'books.db'))
        c = conn.cursor()
        
        # check existing books by book_id
        c.execute('select book_id from goodreads_books')
        existing_books = np.array(c.fetchall())
        
        retrieved_books = np.array(df['book_id']) # books returned by request
        new_books = retrieved_books[~np.isin(retrieved_books, existing_books)]
        # TODO write only new books to DB
        df.to_sql('goodreads_books', conn, index='False', if_exists='append')
        conn.commit()
        conn.close()
    
if __name__ == '__main__':
    asyncio.run(main())