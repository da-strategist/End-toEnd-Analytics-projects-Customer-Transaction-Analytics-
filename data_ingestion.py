import requests
import os
import zipfile
import pandas as pd
import numpy as np
import constants
from kaggle.api.kaggle_api_extended import KaggleApi
import psycopg2


## we start with ingesting our dataset from kaggle

#data ingestion

api = KaggleApi()
api.authenticate()

files = ['transactions.csv', 
         'customers.csv',
         'foreign_customer_dataset.csv', 
         'fraud_dataset.csv' ]

for f in files:
    api.dataset_download_file('tanmayjune/bank-customer-transaction-analysis', 
                          file_name= f, 
                          path= 'raw_data' )
    print(f'{f}: downloaded successfully!!!')
#print(api.dataset_list(search= 'bank-customer-transaction-analysis'))
print('download successful')





    
