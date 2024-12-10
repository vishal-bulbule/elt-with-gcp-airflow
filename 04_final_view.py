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
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define project, dataset, and table details
project_id = 'tt-dev-02'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'  # Main table loaded from CSV
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']  # Country-specific tables to be created
# DAG definition
with DAG(
    dag_id='load_and_transform_view',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery and create country-specific tables',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Task 1: Check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-src-global-data',  # Replace with your bucket name
        object='global_health_data.csv',  # Replace with the file path in the bucket
        timeout=300,  # Maximum wait time in seconds
        poke_interval=30,  # Time interval in seconds to check again
        mode='poke',
    )

    # Task 2: Load CSV from GCS to BigQuery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-data',  # Replace with your bucket name
        source_objects=['global_health_data.csv'],  # Path to your file in the bucket
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        skip_leading_rows=1,  # Skip header row
        field_delimiter=',',
        autodetect=True,
    )

    # Tasks 3: Create country-specific tables and store them in a list
    create_table_tasks = []  # List to store tasks
    create_view_tasks = []  # List to store view creation tasks
    for country in countries:
        # Task to create country-specific tables
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,  # Use standard SQL syntax
                }
            },
        )

        # Task to create view for each country-specific table with selected columns and filter
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}_table',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        SELECT 
                            `Year` AS `year`, 
                            `Disease Name` AS `disease_name`, 
                            `Disease Category` AS `disease_category`, 
                            `Prevalence Rate` AS `prevalence_rate`, 
                            `Incidence Rate` AS `incidence_rate`
                        FROM `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = False
                    """,
                    "useLegacySql": False,
                }
            },
        )

        # Set dependencies for table creation and view creation
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)
        
        create_table_tasks.append(create_table_task)  # Add table creation task to list
        create_view_tasks.append(create_view_task)  # Add view creation task to list

    # Dummy success task to run after all tables and views are created
    success_task = DummyOperator(
        task_id='success_task',
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery
    for create_table_task, create_view_task in zip(create_table_tasks, create_view_tasks):
        create_table_task >> create_view_task >> success_task
