# -*- coding: utf-8 -*-
import scrapy

class BookSpiderSpider(scrapy.Spider):
    name = 'brent-weeks'
    allowed_domains = ['www.brentweeks.com']
    start_urls = ['http://www.brentweeks.com/series/the-night-angel-trilogy/', 'http://www.brentweeks.com/series/the-lightbringer-series/']
    
    exclusions = ['The Way of Shadows (Graphic Novel)',
                  'Night Angel Trilogy 10th Anniversary Edition',
                  'Night Angel: The Complete Trilogy']

    def parse(self, response):
        print('\n')
        print('HTTP STATUS: '+str(response.status))
        print(response.css('.post h2 a::text').getall())
        print('\n')
