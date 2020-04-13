from pathlib import Path
import sqlite3

import click

@click.command()
@click.option('--author', '-a', required=True)
@click.option('--book-title', '-b', required=True)
def del_b(author: str, book_title:str):
    '''Mark book from scrapy table to be ignored.'''
    with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
        c = conn.cursor()
        c.execute('''
        DELETE FROM final_books
        WHERE author = ? AND book_title = ?
        ''', (author, book_title)
        )
    conn.close()
    
if __name__ == '__main__':
    del_b()