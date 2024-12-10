"""
Author: [Vishal Bulbule]
Date: [2024-12-10]
Purpose: This DAG script demonstrates how to load and transform data from GCS to BigQuery 
         and create country-specific tables and views for reporting. 
         The code is featured in the YouTube video: "[Video Title]". 

# NOTE:
This script contains placeholder values that you need to replace with your actual values 
to make it functional in your environment.

1. Replace the following placeholders:
   - `project_id`: Your Google Cloud Project ID.
   - `dataset_id`: The name of the staging dataset where raw data will be loaded.
   - `transform_dataset_id`: The dataset name where transformed data will be stored.
   - `reporting_dataset_id`: The dataset name for the final reporting views.
   - `bucket`: The name of the GCS bucket where your CSV file is stored.
   - `object`: The path to the CSV file in the GCS bucket.
   - `countries`: List of country names (modify as per your requirements).

2. Ensure the following:
   - The service account used for Airflow has permissions for GCS and BigQuery operations.
   - The datasets (`staging_dataset`, `transform_dataset`, `reporting_dataset`) exist in BigQuery.

3. Adjust the DAG schedule and other parameters as needed for your use case.

For more details, refer to the video description or comments section.

DISCLAIMER:
This code is for educational purposes only. Modify and test it thoroughly before using it in production.

Happy Coding!
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='check_load_csv_to_bigquery',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery',
    schedule_interval=None,  # Set as required (e.g., '@daily', '0 12 * * *')
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Task to check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-src-global-data',  # Replace with your bucket name
        object='global_health_data.csv',  # Replace with the file path in the bucket
        timeout=300,  # Maximum wait time in seconds
        poke_interval=30,  # Time interval in seconds to check again
        mode='poke',  # Use 'poke' mode for synchronous checking
    )

    # Task to load CSV from GCS to BigQuery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-data',  # Replace with your bucket name
        source_objects=['global_health_data.csv'],  # Path to your file in the bucket
        destination_project_dataset_table='tt-dev-02.staging_dataset.global_data',  # Replace with your project, dataset, and table name
        source_format='CSV', 
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        skip_leading_rows=1,  # Skip header row
        field_delimiter=',',  # Delimiter for CSV, default is ','
        autodetect=True,  # Automatically infer schema from the file
        #google_cloud_storage_conn_id='google_cloud_default',  # Uncomment and replace if custom GCP connection
        #bigquery_conn_id='google_cloud_default',  # Uncomment and replace if custom BigQuery connection
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery