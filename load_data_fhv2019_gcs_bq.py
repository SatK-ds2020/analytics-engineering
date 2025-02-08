import os
import urllib.request
import gzip
import time
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery, storage
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = os.getenv("BASE_URL")
FILE_NAMES = os.getenv("FILE_NAMES")
MONTHS = os.getenv("MONTHS")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
CHUNK_SIZE = 8 * 1024 * 1024
BUCKET_NAME = os.getenv("BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

# make directory for data downloads
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

#If you authenticated through the GCP SDK you can comment out these two lines
GOOGLE_APPLICATION_CREDENTIALS="D:/DEZommcamp2025/gcp-serviceaccount/gcp-warehouse.json" 
client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
# Initialize BigQuery and Storage clients
bucket = client.bucket(BUCKET_NAME)
bigquery_client =  bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)


def download_and_unzip_file(month):
    url = f"{BASE_URL}fhv_tripdata_2019-{month}.csv.gz"
    gz_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")
    csv_file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, gz_file_path)
        print(f"Downloaded: {gz_file_path}")
        
        # Unzip the file
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(csv_file_path, 'wb') as f_out:
                f_out.write(f_in.read())
        print(f"Unzipped: {csv_file_path}")
        
        return csv_file_path
    except Exception as e:
        print(f"Failed to download or unzip {url}: {e}")
        return None
    
def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")
    
    
# Define the schema for the bq table

schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("PUlocationID", "INTEGER"),
    bigquery.SchemaField("DOlocationID", "INTEGER"),
    bigquery.SchemaField("SR_Flag", "INTEGER"),
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
   
]
def create_bq_table_if_not_exists(dataset_id, table_id, schema):
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        bigquery_client.get_table(table_ref)  # Try to get the table
        print(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print(f"Created table in BQ Succesfully {table.project}.{table.dataset_id}.{table.table_id}")
        
    
def load_data_from_gcs_to_bq(bucket_name, file_names, dataset_id, table_id):

    for file_name in file_names:
        uri = f"gs://{bucket_name}/{file_name}"
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )

        load_job = bigquery_client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )

        load_job.result()  # Waits for the job to complete
        print(f"Loaded {load_job.output_rows} rows from {uri} to {dataset_id}.{table_id}"  ) 
    


if __name__ == "__main__":
    # Initialize the GCS bucket here
    #bucket = client.bucket(BUCKET_NAME)
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

        print("All files processed and verified.")
    create_bq_table_if_not_exists(DATASET_ID, TABLE_ID, schema)   
    load_data_from_gcs_to_bq(BUCKET_NAME, FILE_NAMES, DATASET_ID, TABLE_ID)
    
    print("All files uploaded to BigQuery successfully")
    