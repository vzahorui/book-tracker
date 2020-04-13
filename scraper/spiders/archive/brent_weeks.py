from datetime import datetime
from unicodedata import normalize

import scrapy

from scraper.items import BookItem


class BrentWeeks(scrapy.Spider):
    name = 'brent_weeks'

    start_urls = ['http://www.brentweeks.com/']

    # step 1: get series 
    def parse(self, response):
        for i in response.xpath("//li[@id='menu-item-15103']/ul/li/a"):
            series_title = i.xpath('./@title').get()
            link_for_series = i.xpath('./@href').get()
            # follow the link to the series and call parse_series
            yield response.follow(link_for_series, 
                                  callback=self.parse_series, 
                                  cb_kwargs = dict(series_title = series_title)
            ) 
    
    # step 2: get books for each series
    def parse_series(self, response, series_title):
        for i in response.xpath("//div[@class='post']/h2/a"):
            book_title = i.xpath('./text()').get()
            link_for_book = i.xpath('./@href').get()
            exclusions = ['The Way of Shadows (Graphic Novel)',
                          'Night Angel Trilogy 10th Anniversary Edition',
                          'Night Angel: The Complete Trilogy'
            ]
            if book_title not in exclusions:
                # follow the link to the book and call parse_pub_date
                yield response.follow(link_for_book,
                                      callback = self.parse_pub_date,
                                      cb_kwargs = dict(series_title = series_title,
                                                       book_title = book_title
                                      )
                )
    
    # step 3: get publication date for each book
    def parse_pub_date(self, response, series_title, book_title):
        parsed_date = response.xpath("//article[@class='post']/p[not(@*)][1]/span[1]/text()").get()
        if parsed_date is not None:
            parsed_date = normalize('NFKD', parsed_date).strip()
        else:
            parsed_date = 'Unknown'
        
        item = BookItem()
        # assign all the values to scrapy item class for containerizing
        item['author'] = 'Brent Weeks'
        item['book_title'] = book_title
        item['series_title'] = series_title
        item['pub_date'] = parsed_date
        item['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        yield item