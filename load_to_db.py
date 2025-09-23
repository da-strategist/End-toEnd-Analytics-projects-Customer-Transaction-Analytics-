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

with open('customers.csv', 'r') as f:
    next(f)
    curr.copy_expert(
        """
    COPY de_learner.customers(customer_id," \
                            "cus_DOB," \
                            "cus_gender," \
                            "cus_location," \
                            "cus_balance")
    FROM STDIN WITH CSV"""
                            ,
                            f
    )
conn.commit()
curr.close()
conn.close()





