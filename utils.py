import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

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

# def get content for mof.gov.vn news
def mof_content(url, max_retries=10, retry_delay=3):
    content_list = []
    publish_date = []
    retries = 0
    paper_link = 'https://mof.gov.vn' + url
    while retries < max_retries:
        try:
            response = requests.get(paper_link)
            bts = BeautifulSoup(response.content, "html.parser")
                
            for content in bts.find_all('p'):
                content_list.append(content.text.strip())
            
            for date_time in bts.find('small'):
                publish_date.append(date_time.text.strip())
                
            break  # Successfully retrieved the content, exit the loop
        except (ConnectionError, Timeout) as e:
            print(f"ConnectionError occurred: {e}. Retrying in {retry_delay} seconds...")
            retries += 1
            time.sleep(retry_delay)

    return ' '.join(content_list), ''.join(publish_date)

# def format date for mof.gov.vn news
def format_date_mof (date_string):
    # Remove trailing whitespace from the date string
    date_string = date_string.strip()
    date_format = '%d/%m/%Y %H:%M:%S'
    date_format = '%d/%m/%Y %H:%M:%S'

    # Convert the string to a datetime object
    date_object = datetime.strptime(date_string, date_format)

    # Extract the date from the datetime object
    formatted_date = date_object.strftime('%Y-%m-%d')
    return formatted_date

# def create dataframe for mof.gov.vn news
def df_mof(url):
    PAPER = []
    NEWS = []
    URL = []
    TIME = []
    CONTENT = []
    time_new = ''
    main_text = ''

    response = requests.get(url)   
    bts = BeautifulSoup(response.content, "html.parser")
    for item in bts.find_all('h4'):
        item_url = item.a['href']
        try:
            content_all = mof_content(item_url)                                 # get all content include time and main text
            main_text = content_all[0].strip()                                  # get content of new
            time_new = content_all[1] if len(content_all[1]) > 0 else ''        # get published date or time of new
            if content_all is not None:
                print('-----------------------------------reading content sequence:', len(main_text))
        except Exception as e:
            print(f"An error occurred for URL: {url}. Skipping this row.")
            print(f"Error: {str(e)}")
        
        PAPER.append('mof.gov.vn')    # append default source from cafef.vn
        NEWS.append(item.a.text)    # append Tiltle of new
        URL.append(item_url)        # append link of new
        TIME.append(time_new)
        CONTENT.append(main_text)
        time.sleep(2)           # 2-second delay between iterations
    
    df_mof = { 
                "PAPER": PAPER, 
                "NEWS": NEWS, 
                "URL": URL,
                "TIME": TIME,
                "CONTENT": CONTENT
                    }
    return pd.DataFrame(df_mof)

# def get content for moit.gov.vn news
def moit_content(url, max_retries=10, retry_delay=3):
    content_list = []
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url)
            bts = BeautifulSoup(response.content, "html.parser")
            
            for content in bts.find_all('p'):
                content_list.append(content.text)
            break  # Successfully retrieved the content, exit the loop
        except (ConnectionError, Timeout) as e:
            print(f"ConnectionError occurred: {e}. Retrying in {retry_delay} seconds...")
            retries += 1
            time.sleep(retry_delay)

    return ' '.join(content_list)

# def create dataframe for moit.gov.vn news

def df_moit(url, page_no):
    PAPER = []
    NEWS = []
    URL = []
    TIME = []
    CONTENT = []

    payload = {
    "layout": "Content.Article.News.default",
    "itemsPerPage": 12,
    "orderBy": "publishTime DESC",
    "pageNo": page_no,
    "service": "Content.Article.selectAll",
    "widgetCode": "5b72a94b9218655475508114",
    "parentId": 5238203,
    "type": "Article.News",
    "categoryId": 5238203,
    "widgetTemplateId": "5feffbc0cccf1c7cdf7dada3",
    "imageSizeRatio": "3:2",
    "hiddenAuthor": 1,
    "hiddenReadMore": 1,
    "page": "Article.News.list",
    "modulePosition": 23,
    "moduleParentId": 12,
    "phpModuleName": "Content.Listing",
    "_t": 1717811711818
}

    response = requests.get(url, params=payload) 
    bts = BeautifulSoup(response.content, "html.parser")
    
    for item in bts.find_all('a', class_='article-title common-title'):           
        item_url = item.get('href')
        next_div = item.find_next_sibling('div')  # Find the next sibling div
        if next_div:
            time_span = next_div.find('span', class_='article-date')  # Find the span within the div
            if time_span:
                time_new = time_span.get_text().strip()  # Get the text content of the found span
            else:
                time_new = 'Error in processing'  # Set to "Error in processing" if time_span is not found
        else:
            time_new = 'Error in processing'  # Set to "Error in processing" if next_div is not found
        
        try:
            main_text = moit_content('https://moit.gov.vn' + item_url).strip()
            if main_text is None or main_text == '':
                main_text = 'Have an error in processing'
            else:
                print('-----------------------------------reading content sequence:', len(main_text))
        except Exception as error:
            print(f"An error occurred for URL: {url}. Skipping this row.")
            print(f"Error: {str(error)}")
            main_text = 'Have an error in processing'
        
        PAPER.append('moit.gov.vn')
        NEWS.append(item.text)
        URL.append('https://moit.gov.vn' + item_url)
        TIME.append(time_new)
        CONTENT.append(main_text)
        # time.sleep(2)  # 1-second delay between iterations
    
    df_moit = { 
                "PAPER": PAPER, 
                "NEWS": NEWS, 
                "URL": URL,
                "TIME": TIME,
                "CONTENT": CONTENT
              }
    return pd.DataFrame(df_moit)

