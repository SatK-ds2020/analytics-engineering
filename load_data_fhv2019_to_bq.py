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

def create_bq_table_with_schema(dataset_id, table_id, schema):
    
    # Create the table with the specified schema
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    # Create the table with the specified schema
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)

    print(f"Created table in BQ {table.project}.{table.dataset_id}.{table.table_id}")


def upload_to_bigquery(file_paths):
    for file_path in file_paths:
        try:
            # Load CSV data into a DataFrame
            df = pd.read_csv(file_path)
            
            # Convert datetime columns to the correct data type
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
            
            table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
            
            # Load DataFrame to BigQuery
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for the load job to complete
            
            print(f'Loaded {job.output_rows} rows from {file_path} into {DATASET_ID}.{TABLE_ID}.')
        except Exception as e:
            print(f"Failed to upload data from {file_path} to BigQuery: {e}")


def purge_files(directory):
    try:
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
    except Exception as e:
        print(f"Failed to purge files: {e}")


if __name__ == "__main__":
    
    # create table in Bigquery warehouse
    create_bq_table_with_schema(DATASET_ID, TABLE_ID, schema)
    
    # Download and unzip the files in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_and_unzip_file, MONTHS))

    # Upload the files from Download directory to Bigquery 
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_bigquery, filter(None, file_paths))  # Remove None values
    
      # Purge files in the download directory
    purge_files(DOWNLOAD_DIR)

    print("All files processed, uploaded to BigQuery, and purged from the download directory.")


