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
        SELECT book_title, COALESCE(m.final_name, s.series_title) AS series_title, number_in_series, 
        publication_year, publication_month 
        FROM scraped_books s LEFT JOIN series_mapper m ON s.series_title = m.seen_name
        WHERE author = '{author}'
        AND to_exclude IS NULL
        '''
        gr_table = f'''
        SELECT * FROM goodreads_books 
        WHERE author_name = '{author}'
        '''
        scrapy_df = pd.read_sql(scrapy_table, conn)
        scrapy_df = scrapy_df.loc[~scrapy_df['book_title'].isin(existing_books)]
        gr_df = pd.read_sql(gr_table, conn)
        gr_df = gr_df.loc[~gr_df['title_without_series'].isin(existing_books)].reset_index(drop=True)
        logger.debug(f"Scraped books for adding to the final table:\n{scrapy_df['book_title'].values}")
        logger.debug(f"Goodreads books for adding to the final table:\n{gr_df['title_without_series'].values}")
        # extracting series name and number in series from Goodreads data
        gr_series = []
        for i in gr_df.index:
            title_len = len(gr_df.loc[i, 'title_without_series'])
            series = gr_df.loc[i, 'book_title'][title_len:]
            gr_series.append(series)
        gr_series = pd.Series(gr_series)
        gr_df['series_title'] = gr_series.str.extract('\((.+)\s+#.*\)', expand=False).str.rstrip(' ,')
        gr_df['number_in_series'] = gr_series.str.extract('\(.*#([\d\.]+).*\)', expand=False)
        # adding zeroes to months in Goodreads data
        month_mapper = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', 
                        '7': '07', '8': '08', '9': '09', '10': '10', '11': '11', '12': '12'}
        gr_df['publication_month'] = gr_df['publication_month'].map(month_mapper)
        # building words count vectorizer
        vectorizer = CountVectorizer(stop_words=['a', 'the'])
        
        lst_of_dicts = []
        lst_of_dicts_series = [] # for checking already added novels
        for i in scrapy_df.index:
            book = scrapy_df.loc[i, 'book_title']
            logger.debug(f'Book in focus: {book}')
            # make vectors from book titles
            mtrx = vectorizer.fit_transform(np.append(book, gr_df['title_without_series'].values)).toarray()
            # finding cosine similarity between a book from scraped Dataframe and each book in Goodreads Datafrmae
            cos_simils = [cosine_similarity(mtrx[0].reshape(1, -1), mtrx[vector].reshape(1, -1))[0][0] for vector in range(1, len(mtrx))]
            # finding indices in Goodreads Dataframe corresponding the most to the books in scraped Dataframe
            most_simils_idc = [i for i, j in enumerate(cos_simils) if j == max(cos_simils)]
            # fetching appropriate rows from Goodreads Dataframe
            gr_book = gr_df.iloc[most_simils_idc, :].sort_values(['publication_year', 'publication_month', 'book_id']).head(1)
            # putting added series into dictionary fo further search for novels not present in scraped data
            if scrapy_df.loc[i, 'series_title'] is not None:
                dict_added_series = {}
                dict_added_series['series_title'] = gr_book['series_title'].values[0]
                dict_added_series['series_title_scrapy'] = scrapy_df.loc[i, 'series_title']
                dict_added_series['number_in_series'] = gr_book['number_in_series'].values[0]
                logger.debug(f'Added series component: {dict_added_series}')
                lst_of_dicts_series.append(dict_added_series)
            dict_for_df = {}
            dict_for_df['author'] = author
            dict_for_df['book_title'] = book
            # belongs to series in scrapy
            if scrapy_df.loc[i, 'series_title'] is not None:
                dict_for_df['series_title'] = scrapy_df.loc[i, 'series_title']
                logger.debug('Got series title from scrapy.')
                # has series number in Goodreads
                if gr_book['number_in_series'].notnull().values[0] == True and max(cos_simils) > 0.9:
                    dict_for_df['number_in_series'] = gr_book['number_in_series'].values[0]
                    logger.debug('Got series number from Goodreads.')
                elif scrapy_df.loc[i, 'number_in_series'] is not None:
                    dict_for_df['number_in_series'] = scrapy_df.loc[i, 'number_in_series']
                    logger.debug('Got series number from scrapy.')
                else:
                    dict_for_df['number_in_series'] = 'Other stories'
                    logger.debug('Series number for a series is unavailable.')
            # does not belong to any series in scrapy but does in Goodreads
            elif scrapy_df.loc[i, 'series_title'] is None \
            and gr_book['number_in_series'].notnull().values[0] == True and max(cos_simils) > 0.9:
                dict_for_df['series_title'] = gr_book['series_title'].values[0]
                dict_for_df['number_in_series'] = gr_book['number_in_series'].values[0]
                logger.debug('Got series name and number from Goodreads.')
            # does not belong to any series
            else:
                dict_for_df['series_title'] = None
                dict_for_df['number_in_series'] = None
                logger.debug('Series is unavailable.')
            # no publication year in scrapy but there is one in Goodreads
            if scrapy_df.loc[i, 'publication_year'] is None \
            and gr_book['publication_year'].notnull().values[0] == True and max(cos_simils) > 0.9:
                dict_for_df['publication_year'] = gr_book['publication_year'].values[0]
                logger.debug('Got publication year from Goodreads.')
                if gr_book['publication_month'].notnull().values[0] == True:  
                    dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                    logger.debug('Got publication month from Goodreads.')
                else:
                    dict_for_df['publication_month'] = None
                    logger.debug('Publication month unavailable.')
            # has publication year in scrapy
            elif scrapy_df.loc[i, 'publication_year'] is not None:
                # has publication year in Goodreads
                if gr_book['publication_year'].notnull().values[0] == True and max(cos_simils) > 0.9:
                    # Goodreads shows earlier publication year
                    if gr_book['publication_year'].values[0] < scrapy_df.loc[i, 'publication_year']:
                        dict_for_df['publication_year'] = gr_book['publication_year'].values[0]
                        logger.debug('Got publication year from Goodreads.')
                        if gr_book['publication_month'].notnull().values[0] == True:
                            dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                            logger.debug('Got publication month from Goodreads.')
                        else:
                            dict_for_df['publication_month'] = None
                            logger.debug('Publication month unavailable.')
                    # Goodreads year is present but is not earlier than the one in scrapy
                    else:
                        dict_for_df['publication_year'] = scrapy_df.loc[i, 'publication_year']
                        logger.debug('Got publication year from scrapy.')
                        # has publication month in scrapy
                        if scrapy_df.loc[i, 'publication_month'] is not None:
                            # has publication month in Goodreads in the same year
                            # and Goodreads shows earlier publication month
                            if gr_book['publication_month'].notnull().values[0] == True \
                            and gr_book['publication_year'].values[0] == scrapy_df.loc[i, 'publication_year'] \
                            and gr_book['publication_month'].values[0] < scrapy_df.loc[i, 'publication_month']:
                                dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                                logger.debug('Got publication month from Goodreads.')
                            else:
                                dict_for_df['publication_month'] = scrapy_df.loc[i, 'publication_month']
                                logger.debug('Got publication month from scrapy.')
                        # no publication month in scrapy but there is one in Goodreads in the same year
                        elif scrapy_df.loc[i, 'publication_month'] is None \
                        and gr_book['publication_month'].notnull().values[0] == True \
                        and gr_book['publication_year'].values[0] == scrapy_df.loc[i, 'publication_year']:
                            dict_for_df['publication_month'] = gr_book['publication_month'].values[0]
                            logger.debug('Got publication month from Goodreads.')
                        else:
                            dict_for_df['publication_month'] = None
                            logger.debug('Publication month unavailable.')
                # no publication year in Goodreads
                else:
                    dict_for_df['publication_year'] = scrapy_df.loc[i, 'publication_year']
                    logger.debug('Got publication year from scrapy.')
                    if scrapy_df['publication_month'] is not None:
                        dict_for_df['publication_month'] = scrapy_df.loc[i, 'publication_month']
                        logger.debug('Got publication month from scrapy.')
                    else:
                        dict_for_df['publication_month'] = None
                        logger.debug('Publication month unavailable.')
            # no publication year anywhere
            else:
                dict_for_df['publication_year'] = None
                logger.debug('Publication year unavailable.')
                dict_for_df['publication_month'] = None
                logger.debug('Publication month unavailable.')  
            lst_of_dicts.append(dict_for_df)
        final_df = pd.DataFrame(lst_of_dicts)
        added_series_df = pd.DataFrame(lst_of_dicts_series) # for checking already added novels
 
        # checking novels from series not present in scraped data
        gr_add_series_df = gr_df.loc[(gr_df['number_in_series'].notnull()) &
                                    (gr_df['number_in_series'].str.contains('\.', regex=True)) &
                                    ~(gr_df['title_without_series'].isin(final_df['book_title']))]
        if gr_add_series_df.empty or added_series_df.empty:
            logger.debug('No additional novels from Goodreads to add.')
        else:
            add_series_merge = gr_add_series_df.merge(added_series_df, how='left', on=['series_title', 'number_in_series'])
            add_series_merge = add_series_merge.loc[add_series_merge['series_title_scrapy'].isnull()]
            # get scraped series names corresponding to Goodreads series names
            add_series = add_series_merge['series_title'].unique()
            series_mapper = {}
            scrapy_series = scrapy_df.loc[scrapy_df['series_title'].notnull(), 'series_title'].values
            for s in add_series:
                mtrx = vectorizer.fit_transform(np.append(s, scrapy_series)).toarray()
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