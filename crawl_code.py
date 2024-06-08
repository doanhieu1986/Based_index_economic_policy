import requests
from bs4 import BeautifulSoup
import re
import datetime
from datetime import datetime
import pandas as pd
import time
import os
import glob
from requests.exceptions import ConnectionError, Timeout
import utils

def crawl_cafef (base_url, output_file, start_page, end_page):
    # base_url: url with format that need to be crawled
    # output_file: path file and name file that crawled result will be saved to
    # start_page, end_page: the range of pages that need to be crawled

    # Check if the CSV file exists
    if os.path.exists(output_file):
        # Read the DataFrame from the CSV file
        df_cafef = pd.read_csv(output_file)
    else:
        # If the file doesn't exist, create a new DataFrame
        df_cafef = pd.DataFrame(columns=['PAPER', 'NEWS', 'URL', 'TIME', 'CONTENT'])

    for page in range(start_page, end_page + 1):  # Change the loop range as needed (325, 328 missing)
        url = base_url.format(page)
        print('--------Python is reading Cafef website for the page: ', page)
        
        try:
            new_data = utils.df_cafef(url)  # Fetch data using the utils.df_cafef function
            df_cafef = pd.concat([df_cafef, new_data], ignore_index=True)  # Add new data to the df_cafef DataFrame
            time.sleep(3)
            
            # Save the DataFrame to a CSV file after each loop iteration
            df_cafef.to_csv(output_file, index=False)
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            print("Continuing to the next page...")

    # Display the DataFrame after updating
    print(df_cafef)

# crawl_cafef ('https://cafef.vn/timelinelist/18836/{}.chn', '/Users/hieudoan/PROJECTS/INDEX 2023//result/cafef_18836.csv', 328, 328)
# 18836: Doanh nghiep -- 18833: Vi~ mo -- 18834: Tai chinh ngan hang -- 18839: Thi truong

def crawl_mof (base_url, output_file, start_page, end_page):
    # base_url: url with format that need to be crawled
    # output_file: path file and name file that crawled result will be saved to
    # start_page, end_page: the range of pages that need to be crawled

    # Check if the CSV file exists
    if os.path.exists(output_file):
        # Read the DataFrame from the CSV file
        df = pd.read_csv(output_file)
    else:
        # If the file doesn't exist, create a new DataFrame
        df = pd.DataFrame(columns=['PAPER', 'NEWS', 'URL', 'TIME', 'CONTENT'])

    for page in range(start_page, end_page + 1):  # Change the loop range as needed (325, 328 missing)
        url = base_url.format(page)
        print('--------Python is reading MOF website for the page: ', page)
        
        try:
            new_data = utils.df_mof(url)  # Fetch data using the utils.df_mof function
            new_data['TIME'] = new_data['TIME'].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d'))     # convert time format 
            df = pd.concat([df, new_data], ignore_index=True)  # Add new data to the df_cafef DataFrame
            df.drop_duplicates(inplace=True)    # Drop duplicate rows based on the 'NEWS' and 'URL' columns
            df.sort_values(by='TIME', ascending=False, inplace=True)
            time.sleep(3)
            
            # Save the DataFrame to a CSV file after each loop iteration
            df.to_csv(output_file, index=False)
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            print("Continuing to the next page...")

    # Display the DataFrame after updating
    print(df)

crawl_mof('https://mof.gov.vn/webcenter/portal/tttc/pages_r/o/hdntc?selectedPage={}&docType=TinBai&mucHienThi=1902', 
          '/Users/hieudoan/PROJECTS/INDEX 2023//result/mof_hdntc.csv',2,2)

def crawl_moit (base_url, output_file, start_page, end_page):
    # base_url: url with format that need to be crawled
    # output_file: path file and name file that crawled result will be saved to
    # start_page, end_page: the range of pages that need to be crawled
    
    error_page = []

    # Check if the CSV file exists
    if os.path.exists(output_file):
        # Read the DataFrame from the CSV file
        df = pd.read_csv(output_file)
    else:
        # If the file doesn't exist, create a new DataFrame
        df = pd.DataFrame(columns=['PAPER', 'NEWS', 'URL', 'TIME', 'CONTENT'])

    for page in range(start_page, end_page + 1):  # Change the loop range as needed (325, 328 missing)
        print('--------Python is reading MOIT website for the page: ', page)
        
        try:
            new_data = utils.df_moit(base_url, page)  # Fetch data using the utils.df_mof function
            # new_data['TIME'] = new_data['TIME'].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y').strftime('%Y-%m-%d'))     # convert time format 
            df = pd.concat([df, new_data], ignore_index=True)  # Add new data to the df_cafef DataFrame
            df.drop_duplicates(inplace=True)    # Drop duplicate rows based on the 'NEWS' and 'URL' columns
            df.sort_values(by='TIME', ascending=False, inplace=True)
            if len(new_data) == 0:
                error_page.append(page)
            time.sleep(10)
            
            # Save the DataFrame to a CSV file after each loop iteration
            df.to_csv(output_file, index=False)
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            print("Continuing to the next page...")

    # Display the DataFrame after updating
    print(df)
    print('Error pages are (pages are not to be crawled): ', error_page)

# Need update payload for each subjects on moit.gov.vn on utils.df_moit (see appendix payload file)
# crawl_moit("https://moit.gov.vn/tin-tuc/phat-trien-cong-nghiep", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_ptcn.csv",4,4)
# crawl_moit("https://moit.gov.vn/tin-tuc/thi-truong-trong-nuoc", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_tttn.csv",16,20)
# crawl_moit("https://moit.gov.vn/tin-tuc/thi-truong-nuoc-ngoai", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_ttnn.csv",30,35)
# crawl_moit("https://moit.gov.vn/tin-tuc/xuc-tien-thuong-mai", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_xttm.csv",30,30)
# crawl_moit("https://moit.gov.vn/tin-tuc/bao-chi-voi-nguoi-dan", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_bcvnd.csv",9,10)
# crawl_moit("https://moit.gov.vn/bao-ve-nen-tang-tu-tuong-cua-dang", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_bvntttd.csv",1,5)
# crawl_moit("https://moit.gov.vn/tin-tuc/thong-bao", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_tb.csv",26,27)
# crawl_moit("https://moit.gov.vn/quan-ly-thi-truong", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_qltc.csv",8,8)
# crawl_moit("https://moit.gov.vn/tu-hao-hang-viet-nam", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_thhvn.csv",9,10)
# crawl_moit("https://moit.gov.vn/phat-trien-ben-vung", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_ptbv.csv",6,9)
# crawl_moit("https://moit.gov.vn/tin-tuc/chuyen-doi-so", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_cds.csv",1,10)
# "https://moit.gov.vn/khoa-hoc-va-cong-nghe", "/Users/hieudoan/PROJECTS/INDEX 2023//result/moit_KHCN.csv",7,10