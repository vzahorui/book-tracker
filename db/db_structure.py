import os
import sqlite3


conn = sqlite3.connect('books.db')
c = conn.cursor()

with open(os.path.join('sql', 'DDL.sql'), 'r') as f:
    ddl = f.read()

c.executescript(ddl)
conn.commit()
conn.close()