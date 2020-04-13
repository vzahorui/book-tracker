DROP TABLE IF EXISTS goodreads_books;
CREATE TABLE goodreads_books (
    book_id integer primary key,
    book_title text,
    title_without_series text,
    publication_year text, 
    publication_month text,
    author_id integer,
    author_name text,
    created_at text
);

DROP TABLE IF EXISTS scraped_books;
CREATE TABLE scraped_books (
    author text,
    book_title text,
    series_title text,
    number_in_series text,
    publication_year text,
    publication_month text,
    scraped_at text,
    to_exclude integer
);

DROP TABLE IF EXISTS scraped_books_stage;
CREATE TABLE scraped_books_stage (
    author text,
    book_title text,
    series_title text,
    number_in_series text,
    publication_year text,
    publication_month text,
    scraped_at text
);

DROP TABLE IF EXISTS final_books;
CREATE TABLE final_books (
    author text,
    book_title text,
    series_title text,
    number_in_series text,
    publication_year text,
    publication_month text,
    appears_in text
);

DROP TABLE IF EXISTS fantasticfiction_authors;
CREATE TABLE fantasticfiction_authors (
    author text
);

DROP TABLE IF EXISTS series_mapper;
CREATE TABLE series_mapper (
    seen_name text,
    final_name text
);