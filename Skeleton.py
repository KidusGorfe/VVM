#pip install google-cloud-storage pandas requests

import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime

# GOOGLE CLOUD STORAGE UTILS
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# FETCHING DATA UTILS
def fetch_blackbook_data():
    # Replace with the API call specifics
    response = requests.get("BLACKBOOK_API_ENDPOINT")
    return response.json()

def fetch_vin_audit_data():
    # Replace with the API call specifics
    response = requests.get("VINAUDIT_API_ENDPOINT")
    return response.json()

def fetch_auction_data():
    # Replace with the API call specifics
    response = requests.get("AUCTION_API_ENDPOINT")
    return response.json()

def transform_data(data):
    # Dummy transformation - replace with actual transformation logic
    df = pd.DataFrame(data)
    return df

# FETCH, TRANSFORM, AND STORE DATA UTIL
def fetch_store_data(fetch_func, transform_func, filename, bucket_name):
    data = fetch_func()
    df = transform_func(data)
    df.to_csv(filename, index=False)  # Save to CSV
    upload_blob(bucket_name, filename, f"{datetime.now().strftime('%Y-%m-%d')}/{filename}")  # Upload to GCS with the date in the path

if __name__ == "__main__":
    # Google Cloud Storage settings
    BUCKET_NAME = "your_gcs_bucket_name"  # Make sure this bucket already exists in your GCS

    fetch_store_data(fetch_blackbook_data, transform_data, "blackbook_data.csv", BUCKET_NAME)
    fetch_store_data(fetch_vin_audit_data, transform_data, "vinaudit_data.csv", BUCKET_NAME)
    fetch_store_data(fetch_auction_data, transform_data, "auction_data.csv", BUCKET_NAME)

