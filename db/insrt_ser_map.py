from pathlib import Path
import sqlite3

import click


@click.command()
@click.option('--seen-series', '-s', required=True)
@click.option('--final-series', '-f', required=True)
def insrt_ser_map(seen_series: str, final_series: str):
    '''Inserting series names into the mapper table.'''
    with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
        c = conn.cursor()
        c.execute('''
        INSERT INTO series_mapper
        VALUES (?, ?)''', (seen_series, final_series)
        )
    conn.close()
    
if __name__ == '__main__':
    insrt_ser_map()