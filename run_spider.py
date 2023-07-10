
import scrapy
import sqlite3
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from pdf_downloader.spiders.pdf_spider import PDFSpider
from main_site_pdf_downloader.spiders.mainsite_spider import MainsiteSpiderSpider

def run_spider1(runner, cursor):
    try:
        sql1 = """select result_1,result_2,result_3,id_code,domain from company where domain in ('bci.cl', 'coalindia.in') """
        # sql1 = """select result_1,result_2,result_3,id_code,domain from company where (urls is null or urls ='') """ # or urls = 'Timeout, retry failed' """
        data_set1 = cursor.execute(sql1)
    except Exception as e:
        print(f"Error executing SQL1: {e}")
        return

    for item in data_set1:
        result_urls = [url for url in item[:3] if url is not None]
        id_code = item[3]
        domain = item[4]
        try:
            runner.crawl(PDFSpider, result_urls, domain, id_code)
        except Exception as e:
            print(f"Error running PDFSpider: {e}")

    return runner.join()

def run_spider2(runner, cursor):
    try:
        sql2 = """select company_mainsite, id_code,domain from company where domain in ('bci.cl', 'coalindia.in')"""
        # sql2 = """select company_mainsite, id_code,domain from company where  (urls is null or urls ='')"""
        data_set2 = cursor.execute(sql2)
    except Exception as e:
        print(f"Error executing SQL2: {e}")
        return

    for item in data_set2:
        company_mainsite = item[0]
        id_code = item[1]
        domain = item[2]
        try:
            runner.crawl(MainsiteSpiderSpider, company_mainsite, domain, id_code)
        except Exception as e:
            print(f"Error running MainsiteSpiderSpider: {e}")

    return runner.join()

def run_spider():
    try:
        con = sqlite3.connect('data2.db')
        cursor = con.cursor()
    except Exception as e:
        print(f"Error connecting to SQLite: {e}")
        return

    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})

    try:
        settings = get_project_settings()
        runner = CrawlerRunner(settings)
    except Exception as e:
        print(f"Error getting settings or creating runner: {e}")
        return

    semaphore = defer.DeferredSemaphore(1)  
    d = semaphore.run(run_spider1, runner, cursor)
    d.addCallback(lambda _: semaphore.run(run_spider2, runner, cursor))
    # d = semaphore.run(run_spider2, runner, cursor)
    # d.addCallback(lambda _: semaphore.run(run_spider1, runner, cursor))
    d.addBoth(lambda _: reactor.stop())

    reactor.run()

if __name__ == '__main__':
    run_spider()



####below is for testing#######

# from scrapy.crawler import CrawlerProcess
# process = CrawlerProcess(settings={
#     # Your settings
# })

# url_list = ["https://www.bci.cl/investor-relations-eng/files/disclosure-dates-fs-2022-2023"]
# # Pass the parameters as a dictionary
# params = {
#     'urls' : url_list,
#     'domain' :"latamairlinesgroup.net",
#     'id_code' : "COM-0443"
# }

# process.crawl(PDFSpider, **params)
# process.start()

