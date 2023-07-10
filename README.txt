
This spider is part of the Net Zero project and is mainly used to scrape companies' annual or sustainable reports and download :txt via pdf urls.

search_pipe.ipynb: all execution code and use this notebook to run all whole pipeline

run_spider.py: this is to initiate 2 spiders consecutively

urls_to_txt.py: this is to extract texts from pdf urls

pdf_downloader: this folder contains Scrapy Spider for the webpage

main_site_pdf_downloader: this folder contains Scrapy Spider for the company's main site

The text files are saved in the "txt" folder and txt names and pdf URLs are saved in columns "txt_name" and "final_urls" respectively in "Result_HERO_Companies w NZ Targets (06.06.23)_final.xlsx".
