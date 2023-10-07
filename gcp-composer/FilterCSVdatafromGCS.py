from airflow import DAG 
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator

# Define your GCS bucket and file paths
bucket_name = 'lakshmi_2023'
source_blob_name = 'raw/'
destination_blob_name = 'filtered/'

# Define your Airflow DAG
default_args = {
    'owner': 'airflow',
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
	
	# List all the CSV files in the source folder
	blobs = storage_client.list_blobs(bucket_name, prefix=source_folder)
	
	# Loop through each CSV file and process it
	for blob in blobs:
		if blob.name.endswith('.csv'):
			# Read the CSV file directly from GCS into a Pandas DataFrame
			df = pd.read_csv(f'gs://{bucket_name}/{blob.name}')
			
			# Select only the first 37 columns
			selected_columns = df.iloc[:, :37]
			
			# Define the destination blob name in the filtered folder
			destination_blob_name = blob.name.replace(source_folder, destination_folder)
			
			# Write the selected data as a CSV file to a GCS location using df.to_csv
			selected_columns.to_csv(f'gs://{bucket_name}/{destination_blob_name}', index=False)
			
			print(f'Selected columns saved to: gs://{bucket_name}/{destination_blob_name}')
	


# PythonOperator to process the CSV
process_csv_task = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv,
    dag=dag,
)

# Define the task dependencies
process_csv_task