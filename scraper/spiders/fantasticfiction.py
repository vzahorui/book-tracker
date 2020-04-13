from datetime import datetime
from unicodedata import normalize

import scrapy

from scraper.items import BookItem


class FantasticFiction(scrapy.Spider):
    name = 'fantasticfiction'
    author = 'some_author' # is being set through assignment in master_of_spiders
    start_urls = ['some_webpage'] # is being set through assignment in master_of_spiders
    existing_books = ['some_books'] # is being set through assignment in master_of_spiders
    
    # step 1: crawl initial author's page 
    def parse(self, response):
        url_suffix = '/' + '/'.join(self.start_urls[0].split('/')[-3:])
        # non-fiction and books edited
        exclusions = response.xpath("//div[@class='ff']/div[@class='sectionhead' and (contains(text(), 'Non fiction') or contains(text(), 'edited'))]/following-sibling::div[@class='sectionleft']/a/text()").getall()
        exclusions.extend(self.existing_books)

        for section in response.xpath("//div[@class='sectionleft']"):
            series_title = section.xpath("./a/span/text()").get()
            if series_title is not None:
                series_title = series_title.strip()
            for book in section.xpath("./a[text()]"):
                # book not in exclusions list and the href is not for another author's page
                if book.xpath("./text()").get() not in exclusions \
                and book.xpath("./@href").get().startswith(url_suffix):
                    book_title = book.xpath("./text()").get()
                    book_href = book.xpath("./@href").get()
                    aux_span_id = book.xpath("./preceding-sibling::span[position()=1]/@id").get()
                    number_in_series_query = f"./preceding-sibling::text()[position()=1][preceding-sibling::span[@id='{aux_span_id}']]"
                    number_in_series = book.xpath(number_in_series_query).get()
                    if number_in_series is not None:
                        number_in_series = number_in_series.replace('. ', '').strip()
                    yield response.follow(book_href, 
                                          callback=self.parse_book_page,
                                          cb_kwargs = dict(series_title = series_title,
                                                           book_title = book_title,
                                                           number_in_series = number_in_series,
                                                           author = self.author)
                                         )
            
    # step 2 crawl book's page and retrieve the earliest publication date
    def parse_book_page(self, response, series_title, book_title, number_in_series, author):
        publication_year_head = response.xpath("//div[@class='bookheading']/span/a/text()").get()
        pub_dates = response.xpath("//div[@class='sectionhead']/text()").getall()
        
        months_to_number = {'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
                        'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12'}
        dates_container = [publication_year_head + '.13']
        for i in pub_dates:
            # only containing digits and months names
            if any(word.isdigit() for word in i.split()) \
            and any(word for word in i.split() if word in months_to_number.keys()): # exclude items which do not have any months strings or numbers (years)
                pub_year = [word for word in i.split() if word.isdigit()][0]
                pub_month = [months_to_number.get(word) for word in i.split() if word in months_to_number.keys()][0]
                pub_date_construct = '.'.join([pub_year, pub_month])
                dates_container.append(pub_date_construct)
        pub_date = min(dates_container)
        publication_year, publication_month = pub_date.split('.')
        if publication_month == '13':
            publication_month = 'Unknown'
        
        item = BookItem()
        # assign all the values to scrapy item class for containerizing
        item['author'] = author
        item['book_title'] = book_title
        item['series_title'] = series_title
        item['number_in_series'] = number_in_series
        item['publication_year'] = publication_year
        item['publication_month'] = publication_month
        item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        yield item
   
