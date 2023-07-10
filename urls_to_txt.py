"""
urls_to_txt.py
"""
import os
# path = r"C:\Users\May Xiao\OneDrive\Desktop\arbro\net_zero\merged_v3"
# os.chdir(path)
print(os.getcwd())

import requests
from io import BytesIO
from tika import parser
# import ocrmypdf 
# import sqlite3
import pandas as pd
import random
from urllib.parse import urlparse
from retrying import retry


# db_path = r'C:\Users\May Xiao\OneDrive\Desktop\arbro\net_zero\merged_v3'

# con = sqlite3.connect('data2.db')
# sql =  "select * from company where urls is not null or urls !=''"
# df = pd.read_sql(sql,con=con)
# df.columns
# df = df[['domain','urls','actor_name']]
# # df.urls.values
# con.close()
# df.urls.values


user_agents = [
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko',
'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
]

# Define a retrying decorator with exponential backoff.
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def make_request(url, headers):
    return requests.get(url, headers=headers, timeout=120)

def replace_from_right_once(original_str, old_substring, new_substring):
    index = original_str.rfind(old_substring)
    if index != -1:
        new_str = original_str[:index] + new_substring + original_str[index+len(old_substring):]
        return new_str
    else:
        return original_str
    
def pdf_to_text(df, df_url):
    # Initialize an empty list to store the names of the txt files.
    txt_file_names = []
    import os
    # Directory where the downloaded PDFs and converted txt files will be stored.
    os.makedirs('pdfs', exist_ok=True)
    os.makedirs('txts', exist_ok=True)

    for idx, row in df.iterrows():
        url = row[df_url]
        print(row['actor_name'] + " is running....")
        actor_name = row['actor_name'].replace('/','')
        # Randomly select a user agent.
        headers = {
            'User-Agent': random.choice(user_agents),
        }

        try:
            # Get the name of the PDF file from the URL.
            pdf_file_name = urlparse(url).path.split('/')[-1]
            pdf_file_path = f'pdfs/{pdf_file_name}'

            # If the PDF file does not exist or the 'txt_name' is None, download the PDF.
            if not os.path.exists(pdf_file_path) or pd.isnull(row['txt_name']):
                # Send the request with the user agent set in the headers.
                response = make_request(url, headers)
                response.raise_for_status()  # Check if the request was successful

                # Save the PDF file.
                with open(pdf_file_path, 'wb') as f:
                    f.write(response.content)

            # Create the name for the .txt file.
            # txt_file_name = f'{row["actor_name"]}_{pdf_file_name.replace(".pdf", ".txt")}'
            txt_file_name = f'{actor_name}_{replace_from_right_once(pdf_file_name, ".pdf", ".txt")}'

            txt_file_path = f'txts/{txt_file_name}'

            # If the text file does not exist, but the PDF file exists, extract the text.
            if not os.path.exists(txt_file_path) and os.path.exists(pdf_file_path):
                # Use Tika to parse the PDF.
                print("Pdf is parsing......")
                parsed_pdf = parser.from_file(pdf_file_path)

                # Extract the text content from the parsed PDF.

                pdf_text = parsed_pdf['content']

                # Save the text content to a .txt file.
                print("txt file is saving.....")
                with open(txt_file_path, 'w', encoding='utf-8') as f:
                    f.write(pdf_text.strip())
                print(f"txt file {txt_file_path} saved successfully!")
            # Append the name of the txt file to the list.
            txt_file_names.append(txt_file_name)
            print(actor_name + " finished!!!!!")

        except Exception as e:
            print(f'An error occurred: {e}')
            txt_file_names.append(None)

    # Add a new column 'txt_name' to the dataframe.
    df['txt_name'] = txt_file_names
    print("All pdf to txt finished!!!!!")
    return df


# df = df.iloc[:10]
# pdf_to_text(df)

# df.urls.values
