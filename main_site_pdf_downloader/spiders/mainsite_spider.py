"""
This is google api site scrape script
"""

import scrapy
import time
import csv
import requests
import pandas as pd
from urllib.parse import urljoin
from urllib.parse import urlparse
import random
import re
# from pdfminer.high_level import extract_text
import io
# from io import BytesIO
import tika
from tika import parser
import PyPDF2
import datetime
from scrapy.http import TextResponse
from main_site_pdf_downloader.items import MainSitePdfDownloaderItem
import string


patterns = [
    "sustain",
    "esg",
    "environment",
    "climate",
    "green",
    "responsibility",
    "csr",
    "tcfd",
    "integrated",
    "integrado",
    "annual"
]
            
# xpath_expression = '//a[not(ancestor::head) and not(ancestor::header) and not(ancestor::navigation) and not(ancestor::nav) and not(ancestor::footer)]/@href'
xpath_expression = '//a[not(ancestor::head) and not(ancestor::header) and not(ancestor::footer)]/@href'

# search_terms = [
#     "climate report", "esg report", "integrated report", "csr report", "corporate social responsibility", \
#     "annual report", "sustainability report", "net zero", "environmental", "social", "governance report",\
#     "integrated report", 'commitment'
# ]
search_terms = [ "net zero", "social", "governance report",'commitment']

def is_valid_subpage(url):
    if any(term in url.lower() for term in patterns):
        return True
    return False

def strip_punctuation_and_newlines(text):
    chinese_punctuation = "，。！？【】（）％＃＠＆１２３４５６７８９０：；“”‘’《》"
    translator = str.maketrans('', '', string.punctuation + chinese_punctuation)
    stripped_text = text.translate(translator)
    stripped_text = stripped_text.replace('\n', '')
    return stripped_text

def filter_out_less2page(url):
    response = requests.get(url, verify=False)
    try:
        pdf_content = io.BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_content)
        page_count = len(pdf_reader.pages)
    except:
        page_count = 4
    if page_count > 3:
        return True
    return False

def get_pdf_create_date_page_num(pdf_url):
    try:
        response = requests.get(pdf_url,verify=False)
        pdf_content = io.BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_content)
        metadata = pdf_reader.metadata
        create_date = metadata.get('/CreationDate')
        create_date = int(create_date[2:6])
        page_count = len(pdf_reader.pages)
        return create_date, int(page_count) if page_count else 0
    except:
        try: 
            response = requests.get(pdf_url, verify=False)
            parsed = parser.from_buffer(response.content)
            text = parsed['content']
            pdf_content = io.BytesIO(response.content)
            pdf_reader = PyPDF2.PdfReader(pdf_content)
            page_count = len(pdf_reader.pages)
            text_short = strip_punctuation_and_newlines(text)[:300]
            if '2022' in text_short:
                return 2023, int(page_count) if page_count else 0
            elif '2021' in text_short:
                return 2022, int(page_count) if page_count else 0
            elif '2020' in text_short:
                return 2021, int(page_count) if page_count else 0          
            elif '2019' in text_short:
                return 2020, int(page_count) if page_count else 0              
        except:
            return 2021,2

current_year = datetime.datetime.now().year
numbers = [str(year)[-2:] for year in range(current_year-3, current_year+10)]
regex_patterns = [re.compile(rf"(?=.*{keyword})(?=.*{number})", re.IGNORECASE) for number in numbers for keyword in patterns]

def pdf_url_keywords(pdf_url):
    # for number in numbers:
    #     for i, keyword in enumerate(patterns, start=1):
    #         pattern = rf"(?=.*{keyword})(?=.*{number})"
    for regex_pattern in regex_patterns:
        if regex_pattern.search(pdf_url):
            return pdf_url
            # if re.search(pattern, pdf_url, re.IGNORECASE):
            #     return pdf_url


def check_pdf_file_for_keyword(url):
    """
    Check pdf url created date 22/23 and contains search terms
    """
    response = requests.get(url, verify=False)
    parsed = parser.from_buffer(response.content)
    text = parsed['content']
    create_date = None
    text_short = strip_punctuation_and_newlines(text)[:300]
    try:
        pdf_content = io.BytesIO(response.content)
        pdf_reader = PyPDF2.PdfReader(pdf_content)
        metadata = pdf_reader.metadata
        create_date = metadata.get('/CreationDate')
        create_date = int(create_date[2:6])
        print(create_date)
    except:
        if '2022' in text_short:
            create_date = 2023
        elif '2021' in text_short:
            create_date = 2022
        elif '2020' in text_short:
            create_date = 2021
            print(create_date)
    if len([ i for i in patterns + search_terms if i in text_short.lower()])>0 \
        and (create_date in [2021, 2022, 2023])\
        and ('report' in text_short.lower()):
        return url
    

def find_max_url(row, dictionary):
    allow_domain = row
    # allowed_domain = get_domain(row["website"])
    nested_dict = dictionary.get(allow_domain, {})
    max_first_value = float('-inf')
    max_second_value = float('-inf')
    max_url = None

    for url, (first_value, second_value) in nested_dict.items():
        if first_value > max_first_value:
            max_first_value = first_value
            max_url = url
            # print(max_first_url)

        if first_value == max_first_value and second_value > max_second_value:
            max_second_value = second_value
            max_url = url

    return max_url

class MainsiteSpiderSpider(scrapy.Spider):
    name = "mainsite_spider"
    custom_settings = {
       'DEPTH_LIMIT': 10,
        # 'DEPTH_LIMIT': 5,
        'DOWNLOAD_DELAY': 0,  # Set initial delay to 0, will be overridden in spider_opened method
        # 'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        # 'CLOSESPIDER_PAGECOUNT': 1500,
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
    }
    # allowed_domains = ["www.ccb.com"]
    # start_urls = ["http://www.ccb.com/"]

    
    def __init__(self, urls, domain,id_code):
        super(MainsiteSpiderSpider, self).__init__()
        self.website_urls = urls
        self.id_code = id_code
        self.allowed_domain = domain
        self.pdf_url = ''
        self.pdf_keywords_url_dict = {}
        self.pdf_keywords_url_list_dict = {}
        # self.pdf_url_list = []
        self.start_time = time.time()
        self.limit_in_seconds = 20*60  # 20 mins
        print("int")

    def close_spider(self):
        self.crawler.engine.close_spider(self, 'Desired value found')
        print("close_spider!!!!!")

    def spider_opened(self, spider):
        # Set random download delay when the spider is opened
        download_delay = random.uniform(0.5, 1.5)  # Generate a random delay between 0.5 and 1.5 seconds
        self.custom_settings['DOWNLOAD_DELAY'] = download_delay

    def start_requests(self):
        website_url = self.website_urls
        if 'http' in self.pdf_url:
            print(f"have pdf url: {self.pdf_url} ,stop the request")
            return
        print(website_url)
        response = requests.get(website_url, verify=False)
        print(response.status_code)
        print('--------------')
        print(self.pdf_url)
        if response.status_code == 403:
            self.pdf_url = "Scrapy spider was forbidden by website."
            with open('output.txt', 'a') as file:
                file.write(self.pdf_url)
        else:
            yield scrapy.Request(url=website_url, callback=self.parse, meta={'allowed_domain': self.allowed_domain,'dont_redirect':True})


    def parse(self, response):
        self.pdf_keywords_url_dict = {}
        self.pdf_keywords_url_list_dict = {}
        allowed_domain = response.meta['allowed_domain']

        if response.status == 403:
             self.pdf_url = "Scrape spider was forbidden by website."

        elif 'text/html' in response.headers.get('Content-Type').decode('utf-8') and  len(response.css('a::attr(href)').getall()) == 0:
             self.pdf_url = "No url on this website, need to manually check."

        else:        
            # links = response.xpath(xpath_expression).extract()
            links = response.css('a::attr(href)').getall()
            full_urls = []
            for href in links:
                # to make full url path 
                if href.startswith('http://') or href.startswith('https://'):
                    # If the href is already a full URL, append it as is
                    full_urls.append(href)
                else:
                    # Otherwise, join it with the base_url to create the full URL
                    full_url = urljoin(response.url, href)
                    full_urls.append(full_url)

            for link in full_urls:
                if is_valid_subpage(link) and allowed_domain in link:
                    yield response.follow(link, callback=self.parse_subpage, meta=response.meta)


    def parse_subpage(self, response):
        print("subpage:",response.url)
        pdf_url_list = []


        if 'application/pdf' in response.headers.get('Content-Type').decode('utf-8') or '.pdf' in response.url:
            pdf_url = response.url
            # allowed_domain = response.meta['allowed_domain']
            # new_dict = {allowed_domain: {pdf_url: get_pdf_create_date_page_num(pdf_url)}}
            print("is pdf")
            new_dict = {}
            if filter_out_less2page(pdf_url) and pdf_url not in pdf_url_list:
                print("more than 3 pages")
                matched = False
                for regex_pattern in regex_patterns:
                    if regex_pattern.search(pdf_url):
                        matched = True
                        pdf_url_list.append(pdf_url)
                        break
                
                if matched:
                    new_dict = {self.allowed_domain: {pdf_url: get_pdf_create_date_page_num(pdf_url)}}
                    print(f"Matched, added {pdf_url} into new_dict through URL")

                else:
                    print("Unmatched, going to parsing")
                    response = requests.get(pdf_url, verify=False)
                    parsed = parser.from_buffer(response.content)
                    text = parsed['content']
                    text_short = strip_punctuation_and_newlines(text)[:300]
                    if [i for i in patterns + search_terms if i in text_short.lower() and 'report' in text_short.lower()]:
                            new_dict = {self.allowed_domain: {pdf_url: get_pdf_create_date_page_num(pdf_url)}}
                            print(f"Matched, added {pdf_url} into new_dict through PDF parsing")
                    else:
                        print("PDF doesn't contain keywords....")
            

                try:
                    for key, value in new_dict.items():
                        if key in self.pdf_keywords_url_dict:
                            self.pdf_keywords_url_dict[key].update(value)
                        else:
                            self.pdf_keywords_url_dict[key] = value   
                except:
                    print("PDF and URLs don't contain keywords....")
                # print(len(self.pdf_keywords_url_dict))
                    # self.df['pdf_url'] = self.df.apply(lambda row: find_max_url(row, self.pdf_keywords_url_dict) if pd.isna(row['pdf_url']) else row['pdf_url'], axis=1)
                if len(self.pdf_keywords_url_dict) > 0:
                    self.pdf_url = find_max_url(self.allowed_domain, self.pdf_keywords_url_dict)
                else:
                    self.pdf_url = "There is no url contains keywords or pdf contains keywords, but pdf doesn't contain terms, need to manually check."  
            print(self.pdf_keywords_url_dict)
            print('------------------------')
            if "http" not in self.pdf_url:
                with open('output.txt', 'a') as file:
                    file.write(self.pdf_url)
            else:
                self.close_spider()
                item = MainSitePdfDownloaderItem()
                item['id_code'] = self.id_code
                item['pdf_url'] = self.pdf_url
                yield item

        else:
            # if self.pdf_url:
            #     return  # Stop crawling subpages if pdf_url already exists for the current row
            links = response.xpath(xpath_expression).extract()
            full_urls = [response.urljoin(url) for url in links]
            for link in full_urls:
                if time.time() - self.start_time > self.limit_in_seconds:
                    self.crawler.engine.close_spider(self, 'Time limit reached')
                    self.pdf_url = "Spider reached out 1 hour limit on this domain, need to manually check."
                    break
                if self.allowed_domain in link:
                    try:
                        yield response.follow(link, callback=self.parse_subpage, meta=response.meta)
                    except:
                        print("error:",link)
