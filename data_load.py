import pandas as pd
import psycopg2
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
from db_config import db_config
import constants
from io import BytesIO

conn = psycopg2.connect(
    host = db_config['host'],
    database = db_config['database'],
    port = db_config['port'],
    user = db_config['user'],
    password = db_config['password']
)


data = conn.cursor()
data.execute("SELECT * FROM staging_customer_tbl")
cus_data = data.fetchall()
cus_prod_data = pd.DataFrame(cus_data)
print(cus_prod_data.head())



txn_data = conn.cursor()
txn_data.execute("SELECT * FROM staging_txn_tbl")
txnn_data = txn_data.fetchall()
txn_prod_data = pd.DataFrame(txnn_data)
print(txn_prod_data.head())

connection_string = constants.blob_connect


blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#blob_client = blob_service_client.get_blob_client(container = 'banktxnproj' , blob= 'cus_prod_data' )


###Now we will load out data to our blob storage 

uploads = [
    (cus_prod_data, 'cus_prod_data.csv'),
    (txn_prod_data, 'txn_prod_data.csv')
]

for df, blob_name in uploads:
    buffer = BytesIO()
    data = df.to_csv(buffer, index = False)
    sss = buffer.seek(0)

    blob_client = blob_service_client.get_blob_client(
        container= 'banktxnproj', 
        blob= blob_name
    )
    blob_client.upload_blob(data= buffer, overwrite = True)
    print(f"{blob_name}: Data upload successful")







