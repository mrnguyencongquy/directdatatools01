from google.cloud import storage, datastore

# from google.appengine.api import urlfetch
import os
import pandas as pd
import chardet
import csv, json

# global table_id
# global bucket_name

"""INPUT NAME - ID BUCKET_NAME"""
# bucket_name = 'gcs_data_cleaning_tools01'
# table_id = 'csvdata1.csv'

# link to gcs path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-cleaning-tools02-0d00a43ce501.json"

# Create a client object
storage_client = storage.Client()
datastore_client = datastore.Client()
# Storage_client
# print(dir(storage_client))
"""
Key datastore
"""
# key = datastore_client.key('Org1')
# print('key: ',key)
# org = datastore.Entity(key=key)
# print('org: ',org)
# put db in datastore
# datastore_client.put(org)


class data_cleansing:
    """
    Get all files from the bucket
    """

    def get_files(bucket_name):
        bucket = storage_client.get_bucket(bucket_name)
        filename = [filename.name for filename in list(bucket.list_blobs(prefix=""))]
        return filename

    # Choose file name - get user CouldStoreAPI
    def load_file(bucket_name, filename):
        # {}'.format(table_id)
        delimiter = "/"
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(
            prefix=filename, delimiter=delimiter
        )  # List all objects that satisfy the filter.
        print(blobs)
        # return destination_uri

    """
    Download files
    """

    # Download the file and get respone the encoding data to a destination
    def download_to_local(bucket_name, table_id):
        # The “folder” where the files you want to download are
        folder = "data"
        # {}'.format(table_id)
        delimiter = "/"
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(
            prefix=table_id, delimiter=delimiter
        )  # List all objects that satisfy the filter.
        # Create this folder locally if not exists
        if not os.path.exists(folder):
            os.makedirs(folder)
        # Iterating through for loop one by one using API call
        for blob in blobs:
            destination_uri = "{}/{}".format(folder, blob.name)
            blob.download_to_filename(destination_uri)
            # print(blob.name)
            print(destination_uri)
        return destination_uri

    # Data_check
    def data_check(url_file):
        # print('url_file: ',url_file)
        with open(url_file, "rb") as file:
            file_name = chardet.detect(file.read())
            print("file_name: ", file_name)
        return file_name["encoding"]

    # Save to GG Datastore
    def save_to_datastore(url_file):
        # note that depending on your newline character/file encoding, this may need to be modified
        # In case of missing header in the csv file, we have to pass it explicitly to the program
        csv_file = pd.DataFrame(
            pd.read_csv(url_file, sep=",", header=0, index_col=False)
        )
        print(csv_file.dtypes)
        csv_file.to_json(
            r"url_file.json",
            orient="records",
            date_format="epoch",
            double_precision=10,
            force_ascii=True,
            date_unit="ms",
            default_handler=None,
        )
        datajson = pd.read_json(r"url_file.json", lines=True)

        with open(r"url_file.json") as json_file:
            datajson = json.load(json_file)

        for i in range(0, 25):
            try:
                org = datastore.Entity(datastore_client.key("Org1"))
                # print('datajson: ', datajson[i])
                org.update(datajson[i])

                datastore_client.put(org)
                # print('org: ', org)
            except:
                print("missing")
        # print("org: ", org)

    def query_from_ds(table, key_value):
        query = datastore_client.query(kind=table)
        # query.add_filter("Age", ">", 18)
        # query.add_filter("Gender", "=", "Male")        
        data = list(query.fetch())

        # print("number of columns: ", len(data[0]))
        print("data: ", data)
        return data

    def query_from_ds_matrix(data):
        # query.add_filter("Age", ">", 18)
        # query.add_filter("Gender", "=", "Male")
        # query.order = ["Age"]
        matrix_check = []
        # print("number of columns: ", len(list(results[1].items())))
        for i in range(0, 10):
            print("number of columns: ", len(data[i]))
            matrix_check.append(data[i])
        df = pd.DataFrame(matrix_check)
        print(df)
        # print("number of columns: ", len(results[i]))
        # df = pd.DataFrame.from_dict(results[1].index())

        # print(df)

    def delete_entity(table, key_value):
        delete = datastore_client.query(kind=table)

        entities = list(query_org.fetch())
        for entity in entities:
            datastore_client.delete(entity.key)

        # return query_org


# Save data into Datastore
url_file = data_cleansing.download_to_local(
    "gcs_data_cleaning_tools02", "Combined_DS_v11.csv"
)
# data_cleansing.data_check(url_file)
# data_cleansing.save_to_datastore(url_file)
# bucket_name = 'gcs_data_cleaning_tools01'
# table_id = 'csvdata1.csv' Combined_DS_v10

table = "Org1"
key_value = "Age"
data = data_cleansing.query_from_ds(table, key_value)
# data_cleansing.query_from_ds_matrix(data)
# bucket_name = "gcs_data_cleaning_tools02"
# filename = "Combined_DS_v11.csv"
# filename = data_cleansing.get_files("gcs_data_cleaning_tools02", "Combined_DS_v11.csv")

# print("Filename: ", filename)

# data_cleansing.load_file(bucket_name, filename)
