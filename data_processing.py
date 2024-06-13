import pandas as pd
from datetime import datetime
import os
import glob
import feather

# # Transform sbv dataframe
# ## load dataframe from csv file
# sbv1 = pd.read_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/sbv_content.csv').drop(columns=['Unnamed: 0'])
# sbv2 = pd.read_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/SBV/sbv_all.csv').drop(columns=['Unnamed: 0'])

# ## transform data
# sbv1['TIME'].replace('[()]', '', regex=True, inplace=True)
# sbv1['TIME'] = sbv1['TIME'].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y'))
# sbv1['PAPER'] = 'sbv.gov.vn' # add column to the same structure 
# sbv1 = sbv1[['PAPER'] + [col_name for col_name in sbv1.columns if col_name != 'PAPER']] # order columns name to concat 2 dataframe later
# sbv2['URL'] = 'https://sbv.gov.vn' + sbv2['URL']

# ##concat 2 dataframe
# sbv = pd.concat([sbv1, sbv2], ignore_index=True)
# sbv['TIME'] = sbv['TIME'].apply(lambda x: pd.to_datetime(x, format='%Y/%m/%d'))
# sbv.sort_values(by='TIME', ascending=False, inplace=True)
# sbv.to_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/sbv_updated.csv')
# # print(sbv)

# # Transform cafef dataframe
# ## def concat file csv
# def concat_cafef_file(link):
#     os.chdir(link)
#     all_file_name = [i for i in glob.glob('cafef*.csv')]    # Use glob to match files with the pattern 'cafef%.csv'
#     combined_csv = pd.concat([pd.read_csv(i) for i in all_file_name])    # Read and concatenate all matching CSV files
#     if 'Unnamed: 0' in combined_csv.columns:    # Drop the 'Unnamed: 0' column if it exists
#         combined_csv = combined_csv.drop('Unnamed: 0', axis=1)
#     combined_csv = combined_csv.drop_duplicates()    # Remove duplicate rows
#     return combined_csv

# ## Concat cafef csv file
# cafef1 = concat_cafef_file ('/Users/hieudoan/PROJECTS/INDEX 2023/result/').reset_index(drop=True)
# cafef1['URL'] = 'https://cafef.vn' + cafef1['URL']
# cafef2 = pd.read_feather('/Users/hieudoan/PROJECTS/INDEX 2023/result/CafeF/cafef_all.feather')
# cafef2['URL'] = 'https://cafef.vn' + cafef2['URL']
# cafef2['CONTENT'] = cafef2['CONTENT'].str.strip()

# ## transform data
# cafef = pd.concat([cafef1, cafef2], ignore_index=True)
# cafef['TIME'] = cafef['TIME'].apply(lambda x: pd.to_datetime(x, format='%Y/%m/%d'))
# cafef.sort_values(by='TIME', ascending=False, inplace=True)
# cafef.to_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/cafef_updated.csv')
# print(cafef)

# # transform data vnnet
# ## def convert time
# def convert_to_datetime(value):
#     try:
#         return pd.to_datetime(value, format='%d/%m/%Y')
#     except ValueError:
#         return value
# ## def strip multiple times
# vnnet1 =  pd.read_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/vietnamnet_round2.csv').drop(columns=['Unnamed: 0'])
# vnnet1['CONTENT'] = vnnet1['CONTENT'].str.strip()
# vnnet1['TIME'] = vnnet1['TIME'].apply(convert_to_datetime)
# vnnet2 = pd.read_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/Vietnamnet/vietnamnet_all.csv').drop(columns=['Unnamed: 0'])
# vnnet2['CONTENT'] = vnnet2['CONTENT'].str.strip()
# vnnet2['TIME'] = vnnet2['TIME'].apply(convert_to_datetime)
# vnnet = pd.concat([vnnet1, vnnet2], ignore_index=True)
# vnnet.sort_values(by='TIME', ascending=False, inplace=True)
# vnnet.to_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/vnnet_updated.csv')

# # Transform cafef dataframe
# ## def concat file csv
# def concat_moit_file(link):
#     os.chdir(link)
#     all_file_name = [i for i in glob.glob('moit*.csv')]    # Use glob to match files with the pattern 'cafef%.csv'
#     combined_csv = pd.concat([pd.read_csv(i) for i in all_file_name])    # Read and concatenate all matching CSV files
#     if 'Unnamed: 0' in combined_csv.columns:    # Drop the 'Unnamed: 0' column if it exists
#         combined_csv = combined_csv.drop('Unnamed: 0', axis=1)
#     combined_csv = combined_csv.drop_duplicates()    # Remove duplicate rows
#     return combined_csv

# ## Concat cafef csv file
# moit = concat_moit_file ('/Users/hieudoan/PROJECTS/INDEX 2023/result/').drop(columns=['Unnamed: 0.1']).reset_index(drop=True)
# moit.to_csv('/Users/hieudoan/PROJECTS/INDEX 2023/result/moit_updated.csv')