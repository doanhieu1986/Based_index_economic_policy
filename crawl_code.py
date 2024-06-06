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

base_url = 'https://cafef.vn/timelinelist/18836/{}.chn'                         # change number 18833 or something like that to change topic
output_file = '/Users/hieudoan/PROJECTS/INDEX 2023//result/cafef_18836.csv'     # Path to the CSV file storing the DataFrame with specific topic

# Check if the CSV file exists
if os.path.exists(output_file):
    # Read the DataFrame from the CSV file
    df_cafef = pd.read_csv(output_file)
else:
    # If the file doesn't exist, create a new DataFrame
    df_cafef = pd.DataFrame(columns=['PAPER', 'NEWS', 'URL', 'TIME', 'CONTENT'])

for page in range(1, 501):  # Change the loop range as needed (325, 328 missing)
    url = base_url.format(page)
    print('--------Python is reading for url page', page)
    
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
