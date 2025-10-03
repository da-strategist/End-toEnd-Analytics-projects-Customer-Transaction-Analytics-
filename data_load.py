import pandas as pd
import psycopg2
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
from db_config import db_config
from keys import connection_string, container_name
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
print(cus_data.head())



txn_data = conn.cursor()
txn_data.execute("SELECT * FROM staging_txn_tbl")
txnn_data = txn_data.fetchall()
txn_prod_data = pd.DataFrame(txnn_data)
print(txnn_data.head())


blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(blob_name)


###Now we will load out data to our blob storage 

cus_data_buffer = BytesIO()
cus_data.to_csv(cus_data_buffer, index = False)
cus_data_buffer.seek(0)

blob_client.upload_blob()