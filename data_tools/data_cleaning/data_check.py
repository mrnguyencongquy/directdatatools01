from google.cloud import storage
import os
import pandas as pd
import chardet
import csv

# global table_id
# global bucket_name

"""INPUT NAME - ID BUCKET_NAME"""
# bucket_name = 'gcs_data_cleaning_tools01'
# table_id = 'csvdata1.csv'

# link to gcs path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'data-cleaning-tools01-ec43087a5b92.json'

# Create a client object
storage_client = storage.Client()

# Storage_client
# print(dir(storage_client))

"""
Get all files from the bucket
"""
# bucket = storage_client.get_bucket('gcs_data_cleaning_tools01')
# filename = [filename.name for filename in list(bucket.list_blobs(prefix=''))]

# print(filename)
"""
Download files
"""

# Download the file to a destination 
def download_to_local(bucket_name,table_id):
    # The “folder” where the files you want to download are
    folder='data/{}'.format(table_id)
    delimiter='/'
    bucket=storage_client.get_bucket(bucket_name)
    blobs=bucket.list_blobs(prefix=table_id, delimiter=delimiter) #List all objects that satisfy the filter.
    # Create this folder locally if not exists
    if not os.path.exists(folder):
        os.makedirs(folder)
    # Iterating through for loop one by one using API call
    for blob in blobs:
        destination_uri = '{}/{}'.format(folder, blob.name) 
        blob.download_to_filename(destination_uri)
        print(blob.name)
        with open(destination_uri, 'rb') as file:
            encoding = chardet.detect(file.read())
            encoding['encoding']
            print('encoding: ', encoding['encoding'])

download_to_local('gcs_data_cleaning_tools01',"csvdata1.csv")
# bucket_name = 'gcs_data_cleaning_tools01'
# table_id = 'csvdata1.csv'