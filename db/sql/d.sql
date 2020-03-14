
drop table if exists final_books;
create table final_books (
    author text,
    book_title text,
    series_title text,
    number_in_series text,
    publication_year integer,
    publication_month integer
);