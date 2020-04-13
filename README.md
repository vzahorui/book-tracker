A toolbox which is able to fetch a list of books of a given author, as well as to check new books on the web.

Commands:
 * python open_gsheet.py - opening Google sheet containing books and authors.
 * python list_books.py - list all author's books (available in the local database) on a separate Google sheet.
 * python check_new_books.py - check new books of an author selected at Google sheet cell with validation. New books will be looked for at scraped websites as well as on Goodreads.
 
Goodreads data is used for adding missing info to a scraped book item, such as number in series and publication date. In addition, if a novel belonging to a series is in Goodreads and has its number in series there, it will be added to the database regardless if it is present among scraped books.

**db** folder contains additional command line tools for manually editing database:
 * del_b.py - deleting book from the final table by its author and title.
 * edit_b.py - edit existing book in the final table.
 * insrt_b.py - insert book into the final table.
 * insrt_ff_a.py - insert author's name which is available for scraping at https://www.fantasticfiction.com. 
 * insrt_ser_map.py - insert mapping name for a series which should be further used (for those books which have confusing series names after scraping).
 * mark_scrp_b.py - mark book inside the table of scraped items to be ignored for further processing.
 
All of the listed tools take input options, the meaning of which should be obvious from the source code.

For sci-fi, fantasy and horror additionally consult http://www.isfdb.org regarding short stories. 