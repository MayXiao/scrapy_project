# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
import time


class PdfDownloaderPipeline:
    def process_item(self, item, spider):
        id_code = item['id_code']
        pdf_url = item['pdf_url']
        self.cursor.execute(f" update company set urls = '{pdf_url}' where id_code =  '{id_code}' ")
        print(f"PdfDownloaderPipeline  save url {pdf_url} successfully!!!!!!!!!!!!!!!!!!!!!!!!!!")
        self.connection.commit()
        # while True:
        #     try:
        #         self.connection.commit()
        #         break
        #     except sqlite3.OperationalError:
        #         print("Database is locked. Retrying...")
        #         time.sleep(1)
        # return item

    def open_spider(self, spider):
        print("open_spider")
        self.connection = sqlite3.connect(r"data2.db")
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        id_code = spider.id_code
        self.cursor.execute(f" update company set crawled = 'yes' where id_code =  '{id_code}' ")
        print(f"finish {id_code} close_spider")
        self.connection.commit()
        self.connection.close()
        print("close_spider")