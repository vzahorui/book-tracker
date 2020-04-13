import sqlite3
from pathlib import Path
import logging.config

import yaml
import numpy as np
import pandas as pd

from gsheet_utils import check_author, paste_books


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


def main():
    
    author = check_author() # getting author's name from Google Sheet
    logger.debug(f'Author: {author}')
    with sqlite3.connect(Path(__file__).parent / 'db' / 'books.db') as conn:
        logger.info('Connected to the database.')
        final_table = f'''
                      SELECT book_title, series_title, number_in_series, publication_month, publication_year 
                      FROM final_books
                      WHERE author = '{author}'
                      '''
        df = pd.read_sql(final_table, conn)
        logger.info('DataFrame loaded.')
        # manipulation upon retrieved data
        df.loc[(df['series_title'].notnull()) & (df['number_in_series'].notnull()) &
               (df['number_in_series']!='Other stories'), 'book_title'] = '#' + df['number_in_series'] + ' ' + df['book_title']
        df['series_title'].fillna('Other', inplace=True)
        number_to_month = {'01': 'January', '02': 'February', '03': 'March', '04': 'April', '05': 'May', '06': 'June',
                        '07': 'July', '08': 'August', '09': 'September', '10': 'October', '11': 'November', '12': 'December'}
        df['pub_date'] = df['publication_month'].map(number_to_month).fillna('') + ' ' + df['publication_year'].astype('str').replace('Unknown', '')
        df.sort_values(['series_title', 'number_in_series', 'publication_year', 'publication_month'], inplace=True)
        # creating construct for further input for Google sheet pasting
        series = df['series_title'].unique()
        data_construct = np.array([])
        for s in series:
            header = np.array([s, None, None]).reshape(1, -1) # make series header
            series_books = df.loc[df['series_title']==s, ['book_title', 'pub_date']].values
            series_books = np.concatenate((np.repeat(None, len(series_books)).reshape(-1, 1), series_books), axis=1)
            if data_construct.size == 0:
                data_construct = np.concatenate((header, series_books))
            else:
                data_construct = np.concatenate((data_construct, header, series_books))
        logger.info('Data construct completed.')
        # pasting data to Google sheet       
        paste_books(data_construct.tolist()) 
        logger.info('Data pasted to the Google sheet.')
    conn.close()
    logger.info('Database connection is closed.')
    
if __name__ == '__main__':
    main()