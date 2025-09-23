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
conn.autocommit = True

#dropping test table

curr.execute("DROP TABLE de_learner.customers;") #This deletes the test table created to test connection to our database


#customers table
curr.execute(

    "CREATE TABLE IF NOT EXISTS de_learner.customers (" \
    "customer_id INT PRIMARY KEY," \
    "cus_DOB DATE," \
    "cus_gender VARCHAR(10)," \
    "cus_location VARCHAR(20)," \
    "cus_balance DECIMAL)"
)


# transaction table
curr.execute("DROP TABLE de_learner.transactions;")
curr.execute(
    "CREATE TABLE IF NOT EXISTS de_learner.transactions(" \
    "transactionID INT PRIMARY KEY," \
    "customerID INT, " \
    "transactionDate DATE," \
    "transactionTime TIME," \
    "transactionAmount DECIMAL)"
)

#foreign customer table
curr.execute("DROP TABLE de_learner.forcus;")
curr.execute(
    "CREATE TABLE IF NOT EXISTS de_learner.forcus(" \
    "id INT PRIMARY KEY," \
    "transactionID INT," \
    "customerID INT," \
    "cus_DOB DATE," \
    "cus_gender VARCHAR(10)," \
    "cus_location VARCHAR(20)," \
    "cus_balance DECIMAL,"
    "transactionDate DATE," \
    "transactionTime TIME," \
    "transactionAmount DECIMAL)"
    )

#fraud data table
curr.execute("DROP TABLE de_learner.fraud_tbl;")
curr.execute(
        "CREATE TABLE IF NOT EXISTS de_learner.fraud_tbl (" \
        "id INT PRIMARY KEY," \
        "transactionID INT," \
        "customerID INT," \
        "customerDOB DATE," \
        "cus_gender VARCHAR(10)," \
        "cus_balance DECIMAL,"
        "transactionDate DATE," \
        "transactionTime TIME," \
        "transactionAmount DECIMAL," \
        "age INT," \
        "amntscore_by_loc DECIMAL," \
        "weekday VARCHAR(20)," \
        "isweekend BOOLEAN," \
        "isholiday BOOLEAN," \
        "timeofday VARCHAR(20)," \
        "islatenight BOOLEAN," \
        "timesincelastTxn DECIMAL," \
        "txncountin24hr DECIMAL," \
        "isanomaly INT," \
        "riskscore DECIMAL," \
        "segment VARCHAR(20));"

)
#now we load our data into the database
