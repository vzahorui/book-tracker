from pathlib import Path
import sqlite3
from typing import Optional 

import click


@click.command()
@click.option('--author', '-a', required=True)
@click.option('--book-title', '-b', required=True)
@click.option('--series-title', '-s')
@click.option('--number-in-series', '-n')
@click.option('--publication-year', '-y')
@click.option('--publication-month', '-m')
@click.option('--appears-in')
def insrt_b(author: str, book_title: str, series_title: Optional[str] = None, number_in_series: Optional[str] = None,
                 publication_year: Optional[str] = None, publication_month: Optional[str] = None, appears_in: Optional[str] = None):
    '''Inserting special books into the final table.'''
    with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
        c = conn.cursor()
        c.execute('''INSERT INTO final_books
                     VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                  (author, book_title, series_title, number_in_series,
                   publication_year, publication_month, appears_in)
                 )
    conn.close()
        
if __name__ == '__main__':
    insrt_b()