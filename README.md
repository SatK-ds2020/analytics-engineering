# analytics-engineering

## 1. Set up Google Cloud authentication:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-file.json"
```
## 2. Write the Python script:
What this script does:
Download and Unzip Files: Downloads and unzips the CSV GZ files for each month.

Upload to BigQuery: Uploads the unzipped CSV data to a BigQuery table named fhv_tripdata.

Concurrent Execution: Uses ThreadPoolExecutor for parallel downloading and uploading.

## 3. Upload data to BigQuery from GCS
To upload data from Google Cloud Storage (GCS) to BigQuery, 
## Install the required libraries:
```
pip install google-cloud-bigquery google-cloud-storage
pip install python-dotenv
```

you can use the google-cloud-bigquery library in Python. Here’s a script that will load the files from your GCS bucket into a BigQuery table:
```python
from google.cloud import bigquery
from google.cloud import storage

BUCKET_NAME = 'your-bucket-name'
DATASET_ID = 'your_dataset'
TABLE_ID = 'fhv_tripdata'
FILE_NAMES = [f'fhv_tripdata_2019-{month}.csv' for month in range(1, 13)]

def load_data_from_gcs(bucket_name, file_names, dataset_id, table_id):
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

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
        print(f"Loaded {load_job.output_rows} rows from {uri} to {dataset_id}.{table_id}")

if __name__ == "__main__":
    load_data_from_gcs(BUCKET_NAME, FILE_NAMES, DATASET_ID, TABLE_ID)
```
## Loading Data to BigQuery:
-1. Download and unzip locally from URI and directly uoload to Bigquery. The code link is here:{./{load_data_fhv2019_to_bq.py}}
-2. Upload data to Google Storage bucket forst then upload to Bigquery.The code link is here:{./{load_data_fhv2019_gcs_bq.py}}
-3. Upload by kestra workflow orchestration by backfill schedule for fhv taxi.The code link is here:{./{fhv2019_kestra_scheduled_load.yaml}}
-4. Upload data to gcs and create external table in bigquery. The code link is here:{./{data_upload_gcs.py}}

DBT prject setup:
https://www.youtube.com/watch?v=COeMn18qSkY

Added taxi zone table to Bigquery
```sql
CREATE TABLE `de-zoomcamp2025-448100.trips_data_all.taxizone_data` (
    LocationID INT64, 
    Borough STRING, 
    Zone STRING, 
    service_zone STRING
)
```
Creat External table in Bigquery
```sql
```sql
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.yellow_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://dezoomcamp_skhw4_2025/yellow/*.csv']
);


SELECT COUNT(*) FROM `de-zoomcamp2025-448100.trips_data_all.yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.green_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://dezoomcamp_skhw4_2025/green/*.csv']
);

SELECT COUNT(*) FROM `de-zoomcamp2025-448100.trips_data_all.green_tripdata`;


CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://dezoomcamp_skhw4_2025/fhv/*.csv']
);

SELECT COUNT(*) FROM `de-zoomcamp2025-448100.trips_data_all.fhv_tripdata`;
```

