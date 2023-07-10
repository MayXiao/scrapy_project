# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PdfDownloaderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id_code = scrapy.Field()
    pdf_url = scrapy.Field()


