from pathlib import Path
import sqlite3


with open(Path(__file__).parent / 'sql' / 'DDL.sql', 'r') as f:
    ddl = f.read()
    
with sqlite3.connect(Path(__file__).parent / 'books.db') as conn:
    c = conn.cursor()
    c.executescript(ddl)

conn.close()