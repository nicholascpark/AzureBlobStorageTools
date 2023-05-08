import os, uuid
from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse
from azure.storage.blob import BlobServiceClient, ContainerClient
import pandas as pd
import yaml
from pathlib import Path
import pickle

with open('oef_config_alternative.yaml', 'rb') as file:
    config = yaml.safe_load(file)

storage_connection_str = config['storage_connection_str']
storage_container_name = config['storage_container_name']
storage_account_name = config['storage_account_name']
storage_account_key = config['storage_account_key']

# now supports https and abfss - to expand more.

def azure_upload_df_to_csv(dataframe=None, url=None):
    """
    Upload DataFrame to Azure Blob Storage for given container
    """
    print('Attempting to upload the dataframe to following parquet blob:', url)
    if all([len(dataframe), url]):
        connect_str = storage_connection_str
        container = storage_container_name
        path = urlparse(url).path.split("/")
        if url.startswith('https'):
            blob_path = '/'.join(path[2:])
        elif url.startswith('abfss'):
            blob_path = '/'.join(path[1:])
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
        print('connection established')
        try:
            output = dataframe.to_csv(index=False, encoding="utf-8")
            print('output made complete')
        except Exception as e:
            print('output generation failed')
            print(e)
        try:
            blob_client.upload_blob(output, blob_type="BlockBlob", overwrite = True)
            print('upload complete')
        except Exception as e:
            pass

def azure_download_csv_to_df(url=None):
    """
    Download dataframe from Azure Blob Storage for given url
    Keyword arguments:
    url -- the url of the blob (default None)
    e.g. azure_download_csv_to_df("https://<account_name>.blob.core.windows.net/<container_name>/<blob_name>")
    """
    print('Attempting to read as dataframe:', url)
    try:
        connect_str = storage_connection_str
        path = urlparse(url).path.split("/")
        container = storage_container_name
        if url.startswith('https'):
            blob_path = '/'.join(path[2:])
        elif url.startswith('abfss'):
            blob_path = '/'.join(path[1:])
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_path)
        with BytesIO() as input_blob:
            blob_client.download_blob().download_to_stream(input_blob)
            input_blob.seek(0)
            df = pd.read_csv(input_blob)
        return df
    except Exception as e:
        print(e)
        return None
        

def azure_upload_df_to_parquet(dataframe=None, url=None):

    print('Attempting to upload the dataframe to following parquet blob:', url)
    if all([len(dataframe), url]):
        connect_str = storage_connection_str
        path = urlparse(url).path.split("/")
        container = storage_container_name
        if url.startswith('https'):
            blob_path = '/'.join(path[2:])
        elif url.startswith('abfss'):
            blob_path = '/'.join(path[1:])
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(storage_container_name)

        try:
            parquet_blobs = [blob.name for blob in container_client.list_blobs(name_starts_with = blob_path) if blob.name.endswith('.parquet')]
            assert len(parquet_blobs) == 1, 'Must be maintaining 1 parquet blob in specified blob path'
            blob_client = container_client.get_blob_client(blob = parquet_blobs[0])
            print('connection established')
        except AssertionError as e:
            print(e)
            if len(parquet_blobs) > 1:
                blobs_to_delete = container_client.list_blobs(name_starts_with=blob_path)
                container_client.delete_blobs(*blobs_to_delete)
                upload_path = blob_path + blob_path.split('/')[-2] + '.parquet'
                print(upload_path)
                blob_client = container_client.get_blob_client(blob = upload_path)
            elif len(parquet_blobs) == 0:
                upload_path = blob_path + blob_path.split('/')[-2] + '.parquet'
                print(upload_path)
                blob_client = container_client.get_blob_client(blob = upload_path)
        try:
            parquet_file = BytesIO()
            dataframe.to_parquet(parquet_file)
            parquet_file.seek(0)  
            print('output file generation complete')
        except Exception as e:
            print('output file generation failed')
            print(e)
        try:
            
            blob_client.upload_blob(parquet_file, blob_type="BlockBlob", overwrite = True)
            print('output file upload complete')
        except Exception as e:
            print('output file upload failed')
            print(e)


def azure_download_parquet_to_df(url=None):
    """
    same as above but with parquet paths (not csv files)
    V1: read serially
    """
    # https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-blob/samples/blob_samples_hello_world.py
    # handler=pyarrowfs_adlgen2.AccountHandler.from_account_name('YOUR_ACCOUNT_NAME',azure.identity.DefaultAzureCredential())
    # fs = pyarrow.fs.PyFileSystem(handler)
    print('Attempting to read as dataframe:', url)
    if url:
        connect_str = storage_connection_str
        path = urlparse(url).path.split("/")
        container = storage_container_name
        if url.startswith('https'):
            blob_path = '/'.join(path[2:])
        elif url.startswith('abfss'):
            blob_path = '/'.join(path[1:])
        try:
            container_client = ContainerClient.from_connection_string(conn_str=connect_str, container_name=container)
            print([x for x in container_client.list_blobs(name_starts_with = blob_path)])
            parquet_blobs = [blob.name for blob in container_client.list_blobs(name_starts_with = blob_path) if blob.name.endswith('.parquet')]
            print(parquet_blobs)
            assert len(parquet_blobs) == 1, 'Cannot have more than 1 parquet blob in specified blob path'
            df = pd.concat(
                        objs = [
                                pd.read_parquet(
                                    BytesIO(container_client.download_blob(parquet_blob).readall()), 
                                    engine='pyarrow'
                                    ) 
                                    for parquet_blob in parquet_blobs
                                ],
                        ignore_index = True
                    )
            BytesIO().seek(0)
            return df
        except Exception as e:
            print("Container client failed to generate.")
            print(e)
    else:
        return None

def azure_download_pipeline_from_pickle(url=None):
    """
    same as above but with parquet paths (not csv files)
    V1: read serially
    """
    # https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-blob/samples/blob_samples_hello_world.py
    # handler=pyarrowfs_adlgen2.AccountHandler.from_account_name('YOUR_ACCOUNT_NAME',azure.identity.DefaultAzureCredential())
    # fs = pyarrow.fs.PyFileSystem(handler)

    if url:
        connect_str = storage_connection_str
        path = urlparse(url).path.split("/")
        container = storage_container_name
        if url.startswith('https'):
            blob_path = '/'.join(path[2:])
        elif url.startswith('abfss'):
            blob_path = '/'.join(path[1:])
        try:
            container_client = ContainerClient.from_connection_string(conn_str=connect_str, container_name=container)
            blob_client = container_client.get_blob_client(blob_path)
            print('connection established')
        except Exception as e:
            print(e)
        with BytesIO() as input_blob:
            blob_client.download_blob().download_to_stream(input_blob)
            input_blob.seek(0)
            pipeline = pickle.load(open(pipeline_path, 'rb'))
        return pipeline
    else:
        print('Please provide a URL')
        return None


def azure_download_pipeline_from_pickle(url):
    ''' url: model pipeline pickle blob '''
    if url:
        connect_str = storage_connection_str
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        path = urlparse(url).path.split("/")
        container = path[1]
        blob = '/'.join(path[2:])
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob)
        with BytesIO() as input_blob:
            blob_client.download_blob().download_to_stream(input_blob)
            input_blob.seek(0)
            pipeline = pickle.load(open(pipeline_path, 'rb'))
        return pipeline
    else:
        print('Please provide a URL')
        return None

def azure_download_pipeline_from_pickle_2(url):
    connect_str = storage_connection_str
    path = urlparse(url).path
    path = path.split("/")
    container = path[1]
    blob = '/'.join(path[2:])
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container=container, blob=blob
    )    
    downloader = blob_client.download_blob(0)
    b = downloader.readall()
    pipeline = pickle.loads(b)
    return pipeline

def azure_upload_model_pickle(model_to_upload, blob_path):
    
    # url = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{blob_path}"
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_str)
    blob_client = blob_service_client.get_blob_client(
        container=container, blob=blob_path
    )
    print('connection established')
    try:
        output = pickle.dump(open(model_pipeline, 'wb'))
        blob_client.upload_blob(output, blob_type="BlockBlob", overwrite = True)
    except Exception as e:
        print("Pipeline upload as pickle failed")


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession


def azure_download_parquet_to_df_2(url=None):
    """
    same as above but with parquet paths (not csv files)
    """
    # handler=pyarrowfs_adlgen2.AccountHandler.from_account_name('YOUR_ACCOUNT_NAME',azure.identity.DefaultAzureCredential())
    # fs = pyarrow.fs.PyFileSystem(handler)

    conf = SparkConf().setAppName("session1")
    sc = SparkContext(conf=conf)
    spark_session = SparkSession.builder.appName("session1").getOrCreate()
    spark_session.conf.set("fs.azure.account.key.{storage_account_name}.blob.core.windows.net","{storage_account_key}")

    if url:
        sqlContext = SQLContext(sc)
        sqlContext.read.format('parquet').load(url)        
        return df
    else:
        return None

if __name__ == "__main__":

    file_path = f"{os.getenv('OUTPUT')}"
    print(file_path)



