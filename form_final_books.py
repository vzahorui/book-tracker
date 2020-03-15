import sqlite3
import logging.config
from pathlib import Path

import yaml
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# configuring logging
with open('log_config.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


async def form_final_books(author: str):
    with sqlite3.connect(Path(__file__).parent / 'db' / 'books.db') as conn:
        logger.info('Connected to the database.')
        c = conn.cursor()
        
        # checking already existing in the database books
        c.execute('''SELECT book_title FROM final_books
                     WHERE author = ?''', (author, )
        )
        existing_books = [str(i[0]) for i in c.fetchall()] # making ordinary list from list of tuples
        logger.debug(f'Existing books in the final table:\n{existing_books}')
        
        # fetching data from Goodreads and Scrapy
        scrapy_table = f'''
        SELECT * FROM scraped_books 
        WHERE author = '{author}'
        '''
        gr_table = f'''
        SELECT * FROM goodreads_books 
        WHERE author_name = '{author}'
        '''
        scrapy_df = pd.read_sql(scrapy_table, conn)
        gr_df = pd.read_sql(gr_table, conn)
        logger.debug(f"Scraped books for adding to the final table:\n{scrapy_df['book_title'].values}")
        logger.debug(f"Goodreads books for adding to the final table:\n{gr_df['title_without_series'].values}")
        
        # some data manipulation
        month_strings = ['January', 'February', 'March', 'April', 'May', 'June',
                        'July', 'August', 'September', 'October', 'November', 'December']
        months_to_number = {'January': '1', 'February': '2', 'March': '3', 'April': '4', 'May': '5', 'June': '6',
                        'July': '7', 'August': '8', 'September': '9', 'October': '10', 'November': '11', 'December': '12'}
        re_months = '(' + '|'.join(month_strings) + ')' # construct for passing in regexp
        scrapy_df['publication_month_full'] = scrapy_df['pub_date'].str.extract(re_months)
        scrapy_df['publication_year'] = scrapy_df['pub_date'].str.extract('([0-9]{4})').fillna('Unknown')
        scrapy_df['publication_month'] = scrapy_df['publication_month_full'].map(months_to_number).fillna('Unknown')
        gr_series = []
        for i in gr_df.index:
            title_len = len(gr_df.loc[i, 'title_without_series'])
            series = gr_df.loc[i, 'book_title'][title_len:]
            gr_series.append(series)
        gr_series = pd.Series(gr_series)
        gr_df['series_title'] = gr_series.str.extract('\(([\w\s]+).*#.*\)', expand=False).str.rstrip().fillna('Unknown')
        gr_df['number_in_series'] = gr_series.str.extract('\(.*#([\d\.]+).*\)', expand=False)
        
        # building words count vectorizer
        vectorizer = CountVectorizer(stop_words=['a', 'the'])
        
        lst_of_dicts = []
        lst_of_dicts_series = [] # for checking already added novels
        for i in scrapy_df.index:
            book = scrapy_df.loc[i, 'book_title']
            logger.debug(f'Book in focus: {book}')
            if book not in existing_books:
                logger.debug('New scraped book.')
                # make vectors from book titles
                mtrx = vectorizer.fit_transform(np.append(book, gr_df['title_without_series'].values)).toarray()
                # finding cosine similarity between a book from scraped Dataframe and each book in Goodreads Datafrmae
                cos_simils = [cosine_similarity(mtrx[0].reshape(1, -1), mtrx[vector].reshape(1, -1))[0][0] for vector in range(1, len(mtrx))]
                # finding indices in Goodreads Dataframe corresponding the most to the books in scraped Dataframe
                most_simils_idc = [i for i, j in enumerate(cos_simils) if j == max(cos_simils)]
                # fetching appropriate rows from Goodreads Dataframe
                gr_book = gr_df.iloc[most_simils_idc, :].sort_values(['publication_year', 'publication_month', 'book_id']).head(1)
                # putting added series into dictionary fo further search for novels not present in scraped data
                if scrapy_df.loc[i, 'series_title'] != 'Unknown':
                    dict_added_series = {}
                    dict_added_series['series_title'] = gr_book['series_title'].values[0]
                    dict_added_series['series_title_scrapy'] = scrapy_df.loc[i, 'series_title']
                    dict_added_series['number_in_series'] = gr_book['number_in_series'].values[0]
                    logger.debug(f'Added series component: {dict_added_series}')
                    lst_of_dicts_series.append(dict_added_series)
                dict_for_df = {}
                dict_for_df['author'] = scrapy_df.loc[i, 'author']
                dict_for_df['book_title'] = book
                dict_for_df['series_title'] = scrapy_df.loc[i, 'series_title']
                dict_for_df['number_in_series'] = gr_book['number_in_series'].values[0]
                if scrapy_df.loc[i, 'publication_year'] == 'Unknown':
                    dict_for_df['publication_year'] = gr_book['publication_year'].values[0]
                    logger.debug('Got publication year from Goodreads.')
                else:
                    dict_for_df['publication_year'] = scrapy_df.loc[i, 'publication_year']
                    logger.debug('Got publication year from scraped data.')
                if scrapy_df.loc[i, 'publication_year'] == 'Unknown':
                    dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                    logger.debug('Got publication month from Goodreads.')
                elif scrapy_df.loc[i, 'publication_year'] == gr_book['publication_year'].values[0] and scrapy_df.loc[i, 'publication_month'] == 'Unknown':
                    dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                    logger.debug('Got publication month from Goodreads.')
                else:
                    dict_for_df['publication_month'] = scrapy_df.loc[i, 'publication_month']
                    logger.debug('Got publication month from scraped data.')
                lst_of_dicts.append(dict_for_df)
            else:
                logger.debug('Scraped book already exists in the final table.')
        final_df = pd.DataFrame(lst_of_dicts)
        added_series_df = pd.DataFrame(lst_of_dicts_series) # for checking already added novels
        
        # checking novels not present in scraped data
        gr_add_series_df = gr_df.loc[(gr_df['number_in_series'].notnull()) & 
                                    (gr_df['number_in_series'].str.contains('\.', regex=True)) &
                                    (~gr_df['title_without_series'].isin(existing_books))]
        if gr_add_series_df.empty:
            logger.debug('No additional novels from Goodreads to add.')
        else:
            add_series_merge = gr_add_series_df.merge(added_series_df, how='left', on=['series_title', 'number_in_series'])
            add_series_merge = add_series_merge.loc[add_series_merge['series_title_scrapy'].isnull()]
            # get scraped series names corresponding to Goodreads series names
            add_series = add_series_merge['series_title'].unique()
            series_mapper = {}
            for s in add_series:
                mtrx = vectorizer.fit_transform(np.append(s, scrapy_df['series_title'].values)).toarray()
                cos_simils = [cosine_similarity(mtrx[0].reshape(1, -1), mtrx[vector].reshape(1, -1))[0][0] for vector in range(1, len(mtrx))]
                most_simils_idc = [i for i, j in enumerate(cos_simils) if j == max(cos_simils)]
                scraped_series_name = scrapy_df['series_title'][most_simils_idc[0]]
                series_mapper[s] = scraped_series_name
            add_series_merge['series_title'] = add_series_merge['series_title'].map(series_mapper)
            add_series_merge = add_series_merge[['author_name', 'title_without_series', 'series_title', 'number_in_series', 'publication_year', 'publication_month']]
            add_series_merge.rename(columns={'author_name': 'author', 'title_without_series': 'book_title'}, inplace=True)
            logger.debug(f"Additionally added novels from Goodreads:\n{add_series_merge['book_title'].values}")
            final_df = final_df.append(add_series_merge, ignore_index=True)
        
        # write new books to the database
        if final_df.empty:
            print('No new books.')
            logger.info('Nothing added to the database.')
        else:
            print('New books:')
            for i in final_df.index:
                    print(f"{final_df.loc[i, 'book_title']} by {final_df.loc[i, 'author']}. Publication date - {final_df.loc[i, 'publication_month']}, {final_df.loc[i, 'publication_year']}.")
            final_df.to_sql('final_books', conn, index=False, if_exists='append')
            logger.info('Books added to the database.')
        
    conn.close()
    logger.info('Database connection is closed.')