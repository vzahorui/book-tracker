drop table if exists goodreads_books;
create table goodreads_books (
    book_id integer primary key,
    book_title text,
    title_without_series text,
    publication_year integer, 
    publication_month integer,
    author_id integer,
    author_name text,
    created_at text
);

drop table if exists scraped_books;
create table scraped_books (
    author text,
    book_title text,
    series_title text, 
    pub_date text,
    scraped_at text
);

drop table if exists scraped_books_stage;
create table scraped_books_stage (
    author text,
    book_title text,
    series_title text, 
    pub_date text,
    scraped_at text
);

drop table if exists final_books;
create table final_books (
    author text,
    book_title text,
    series_title text,
    number_in_series text,
    publication_year integer,
    publication_month integer
);