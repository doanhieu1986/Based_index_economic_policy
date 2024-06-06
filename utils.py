import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# def get content for cafef.vn news
def cafef_content(url, max_retries=10, retry_delay=3):
    content_list = []
    publish_date = []
    retries = 0

    while retries < max_retries:
        try:
            # cookie = get_cookie(url)
            # headers = {'Cookie': cookie}
            response = requests.get(url)
            bts = BeautifulSoup(response.content, "html.parser")
            
            for header in bts.find_all('h2', attrs = {"class":"sapo"}):
                content_list.append(header.text)
                
            for content in bts.find_all('p'):
                content_list.append(content.text)
            
            for date_time in bts.find_all('span', attrs = {"class":"pdate"}):
                publish_date.append(date_time.text)
                
            break  # Successfully retrieved the content, exit the loop
        except (ConnectionError, Timeout) as e:
            print(f"ConnectionError occurred: {e}. Retrying in {retry_delay} seconds...")
            retries += 1
            time.sleep(retry_delay)

    return ' '.join(content_list), publish_date

# def format date for cafef.vn news
def format_date_cafef (date_string):
    # Remove trailing whitespace from the date string
    date_string = date_string.strip()
    date_format = '%d-%m-%Y - %H:%M %p'

    # Convert the string to a datetime object
    date_object = datetime.strptime(date_string, date_format)

    # Extract the date from the datetime object
    formatted_date = date_object.strftime('%Y-%m-%d')
    return formatted_date

# def create dataframe for cafef.vn news
def df_cafef(url):
    PAPER = []
    NEWS = []
    URL = []
    TIME = []
    CONTENT = []
    time_new = ''
    response = requests.get(url)   
    bts = BeautifulSoup(response.content, "html.parser")
    for item in bts.find_all('h3'):
        item_url = item.a['href']
        try:
            content_all = cafef_content('http://cafef.vn' + item_url)                           # get all content include time and main text
            main_text = content_all[0].strip()                                                            # get content of new
            time_new = format_date_cafef(content_all[1][0]).replace('\n', '') if len(content_all[1]) > 0 else ''     # get published date or time of new
            if content_all is not None:
                print('-----------------------------------reading content sequence:', len(main_text))
        except Exception as e:
            print(f"An error occurred for URL: {url}. Skipping this row.")
            print(f"Error: {str(e)}")
        
        PAPER.append('cafef.vn')    # append default source from cafef.vn
        NEWS.append(item.a.text)    # append Tiltle of new
        URL.append(item_url)        # append link of new
        TIME.append(time_new)
        CONTENT.append(main_text)
    df_cafef = { 
                "PAPER": PAPER, 
                "NEWS": NEWS, 
                "URL": URL,
                "TIME": TIME,
                "CONTENT": CONTENT
                    }
    return pd.DataFrame(df_cafef)
