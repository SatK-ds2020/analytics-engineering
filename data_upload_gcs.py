import os
import time
import requests
from google.cloud import storage
from io import BytesIO
import gzip

# Load environment variables
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
BUCKET_NAME = 'dezoomcamp_skhw4_2025'
CHUNK_SIZE = 8 * 1024 * 1024
TAXI_TYPES = ["yellow", "green","fhv"]
YEAR = ["2019","2020"]

# If you authenticated through the GCP SDK you can comment out these two lines
GOOGLE_APPLICATION_CREDENTIALS = "D:/DEZommcamp2025/gcp-serviceaccount/gcp-warehouse.json"
client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
# Initialize BigQuery and Storage clients
bucket = client.bucket(BUCKET_NAME)

def download_and_upload_file(year, month, taxi_type):
    url = f"{BASE_URL}/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"
    blob_name = f"{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  

    try:
        print(f"Downloading and uploading {url}...")

        # Stream the data directly to GCS
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Ensure the request was successful

        # Use BytesIO to handle the data in memory
        with BytesIO(response.content) as gz_file:
            with gzip.open(gz_file, 'rb') as f_in:
                blob.upload_from_file(f_in)
        
        print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

    except Exception as e:
        print(f"Failed to download or upload {url}: {e}")

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def main():
    for year in YEAR:
        for taxi_type in TAXI_TYPES:
            for month in MONTHS:
                download_and_upload_file(year, month, taxi_type)
                # Optional: Verify the upload
                if verify_gcs_upload(f"{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv"):
                    print(f"Verification successful for {taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv")
                else:
                    print(f"Verification failed for {taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv")

if __name__ == "__main__":
    main()
