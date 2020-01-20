drop table if exists goodreads_books;
create table goodreads_books (
    book_id integer primary key,
    book_title text,
    publication_year integer, 
    publication_mont integer,
    author_id integer,
    author_name text,
    to_show integer
);