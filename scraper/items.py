import scrapy


class BookItem(scrapy.Item):
    author = scrapy.Field()
    book_title = scrapy.Field()
    series_title = scrapy.Field()
    pub_date = scrapy.Field()
    scraped_at = scrapy.Field()
