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

#cur = conn.cursor()
#conn.autocommit = True #this ensures the output is saved or commited to the database. when the table was created initially, it did not reflect in postgres due to the 
                            #abscence of the autocommit or commit

#test = cur.execute(
 #   "CREATE TABLE de_learner.test_tbl (test_id INT PRIMARY KEY, " \
  #                          "Fname VARCHAR(20), " \
   #                         "Lname VARCHAR(20))"

    #)


curr = conn.cursor()
#conn.autocommit = True

#dropping test table

curr.execute("DROP test_tbl IF EXIST" )


#customers table
curr.execute(

    "CREATE TABLE de_learner.customers (" \
    "customer_id INT PRIMARY KEY," \
    "cus_DOB DATE," \
    "cus_gender VARCHAR(10)," \
    "cus_location VARCHAR(20)," \
    "cus_balance FLOAT)"
)


# transaction table

curr.execute(
    "CREATE TABLE de_learner.transactions(" \
    "TransactionID INT PRIMARY KEY" \
    "CustomerID INT  " \
    "TransactionDate DATE         " \
    "TransactionTime TIME        " \
    "TransactionAmount FLOAT)"
)

#foreign customer table
curr.execute()
#now we load our data into the database
