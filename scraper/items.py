import scrapy


class BookItem(scrapy.Item):
    author = scrapy.Field()
    book_title = scrapy.Field()
    series_title = scrapy.Field()
    number_in_series = scrapy.Field()
    publication_year = scrapy.Field()
    publication_month = scrapy.Field()
    scraped_at = scrapy.Field()
