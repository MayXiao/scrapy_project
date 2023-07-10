# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3

class MainSitePdfDownloaderPipeline:
    def process_item(self, item, spider):
        # return item
        id_code = item['id_code']
        pdf_url = item['pdf_url']
        self.cursor.execute(f" update company set urls = '{pdf_url}' where id_code =  '{id_code}' ")
        print(f"Main site PdfDownloaderPipeline  save url {pdf_url} successfully~~~~~~~~~~~~~~")
        self.connection.commit()
        return item

def open_spider(self, spider):
    print("open_spider")
    self.connection = sqlite3.connect("data2.db")
    self.cursor = self.connection.cursor()

def close_spider(self, spider):
    self.connection.commit()
    self.connection.close()
    print("close_spider")
