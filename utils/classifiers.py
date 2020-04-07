# Import modules
from azure.storage.blob import BlobServiceClient
import os
import pickle
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords


def load_model():
    '''Function to load machine learning model'''
    # Select model for categorization
    local_file_name = 'tfidf-model-2020-03-05.p'
    local_path = ''
    download_file_path = os.path.join(local_path, local_file_name)
    if not os.path.exists(download_file_path):
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ['AZURE_STORAGE_CONNECTION_STRING'])
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(
            container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name)
        print("\nDowloading from Azure Storage blob:\n\t" + local_file_name)
        # Download the blob to a local file
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
    # Load pickled data
    file = open(local_file_name, 'rb')
    model = pickle.load(file)
    file.close()
    # Select vectorizer
    local_file_name = 'vectorizer-2020-03-05.p'
    local_path = ''
    download_file_path = os.path.join(local_path, local_file_name)
    if not os.path.exists(download_file_path):
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ['AZURE_STORAGE_CONNECTION_STRING'])
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(
            container=os.environ['AZURE_STORAGE_CONTAINER'], blob=local_file_name)
        print("\nDowloading from Azure Storage blob:\n\t" + local_file_name)
        # Download the blob to a local file
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
    # Load pickled data
    file = open(local_file_name, 'rb')
    vectorizer = pickle.load(file)
    file.close()
    return model, vectorizer