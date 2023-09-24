from airflow import DAG 
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator

# Define your GCS bucket and file paths
bucket_name = 'lakshmi_2023'
source_blob_name = 'raw/Customers.csv'
destination_blob_name = 'filtered/Customers.csv'

# Define your Airflow DAG
default_args = {
    'owner': 'your_name',
    'start_date': days_ago(1),
}

dag = DAG(
    'load_and_filter_csv',
    default_args=default_args,
    description='Load and filter CSV data from GCS with Airflow',
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Set to True if you want to backfill historic data
    tags=['example'],
)

# Task to process the CSV file
def process_csv():
    # Initialize the GCS client
    storage_client = storage.Client()
    
    # Read the CSV file directly from GCS into a Pandas DataFrame
    df = pd.read_csv(f'gs://{bucket_name}/{source_blob_name}')
    
    # Select only the first 37 columns
    selected_columns = df.iloc[:, :37]
    
    # Write the selected data as a CSV file to a GCS location using df.to_csv
    selected_columns.to_csv(f'gs://{bucket_name}/{destination_blob_name}', index=False)

# PythonOperator to process the CSV
process_csv_task = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv,
    dag=dag,
)

# Define the task dependencies
process_csv_task 