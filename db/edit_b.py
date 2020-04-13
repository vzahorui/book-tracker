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
def edit_b(author: str, book_title: str, series_title: Optional[str] = None, number_in_series: Optional[str] = None,
           publication_year: Optional[str] = None, publication_month: Optional[str] = None, appears_in: Optional[str] = None):
    '''Edit books inside the final table.'''
    script_start = 'UPDATE final_books SET '
    script_end = 'WHERE author = ? AND book_title = ?'
    
    options = [series_title, number_in_series, publication_year, publication_month, appears_in]
    series_part = f"series_title = '{series_title}' ,"
    n_in_series_part = f"number_in_series = '{number_in_series}' ,"
    year_part = f"publication_year = '{publication_year}' ,"
    month_part = f"publication_month = '{publication_month}' ,"
    appear_part = f"appears_in = '{appears_in}' ,"
    script_options = [series_part, n_in_series_part, year_part, month_part, appear_part]
    script_middle = ''
    for o, s in zip(options, script_options):
        if o is not None:
            script_middle = script_middle + s
    if len(script_middle) == 0:
        return
    else:
        script_middle = script_middle[:-1]
        
        script = script_start + script_middle + script_end
        with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
            c = conn.cursor()
            c.execute(script, (author, book_title))
        conn.close()
        
if __name__ == '__main__':
    edit_b()