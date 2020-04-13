from pathlib import Path
import sqlite3

import click


@click.command()
@click.option('--author', '-a', required=True)
def insrt_ff_a(author: str):
    '''Inserting fantasticfiction author into the table.'''
    with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
        c = conn.cursor()
        c.execute('''
        INSERT INTO fantasticfiction_authors
        VALUES (?)''', (author, )
        )
    conn.close()
    
if __name__ == '__main__':
    insrt_ff_a()