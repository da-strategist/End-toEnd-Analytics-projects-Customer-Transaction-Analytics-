import pandas as pd
import psycopg2
from db_config import db_config



#here we setup our connection
conn = psycopg2.connect(
    host = db_config['host'],
    database = db_config['database'],
    port = db_config['port'],
    user = db_config['user'],
    password = db_config['password']
)

curr = conn.cursor()
conn.autocommit = True

curr.execute("SET DATESTYLE TO 'DMY';")

#syntax to copy a single csv file into a table in a db
#with open('customers.csv', 'r') as f:
 #   next(f)
  #  curr.copy_expert(
   #     """
    #COPY de_learner.customers(customer_id," \
     #                       "cus_DOB," \
      #                      "cus_gender," \
       #                     "cus_location," \
        #                    "cus_balance")
    #FROM STDIN WITH CSV"""
     #                       ,
      #                      f
    #)


#now we shall load all files into our database


#we start by creating a dict pointing to our csv files

data_files = {
    "customers": "raw_data/customers.csv",
    "forcus": "raw_data/foreign_customer_dataset.csv",
    "fraud_tbl": "raw_data/fraud_dataset.csv",
    "transactions": "raw_data/transactions.csv"
}

for table, file_path in data_files.items():
    with open(file_path, 'r') as f:
        curr.copy_expert(
        f"""
        COPY de_learner.{table}
        FROM STDIN WITH CSV HEADER""", 
        f

    )

conn.commit()
curr.close()
conn.close()





