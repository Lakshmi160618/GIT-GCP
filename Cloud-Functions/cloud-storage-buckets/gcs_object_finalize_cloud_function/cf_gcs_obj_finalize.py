# main.py
import os

def hello_gcs(data, context):
    """Cloud Function to handle GCS object finalization."""
    file_name = data['name']
    bucket_name = os.environ.get('BUCKET_NAME')

    print(f"File {file_name} finalized in bucket {bucket_name}.")
import os
from google.cloud import bigquery

def gcs_to_bq(event, context):
    file = event
    gcs_uri = file['id']
    gcs_uri = gcs_uri.split("/")
    gcs_uri = "gs://" + gcs_uri[0] + "/" + gcs_uri[1]
    
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Define the dataset and table information
    project_id = os.environ.get('project_id')
    print(project_id)
    dataset_id = os.environ.get('dataset_id')
    print(dataset_id)
    table_id =  gcs_uri[1].split(".")[0]
    print(table_id)

    # Define the GCS URI of the CSV file
    gcs_uri = gcs_uri
    print(gcs_uri)
    
    # Create the dataset if it doesn't exist (optional)
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    if not client.get_dataset(dataset):
        client.create_dataset(dataset)
    
    # Create the table with schema auto-detection
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Enable schema auto-detection
        skip_leading_rows=1,  # If your CSV file has a header, set this to 1
    )
    
    table_ref = dataset_ref.table(table_id)
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    
    load_job.result()  # Wait for the job to complete
    
    print(f"Table {table_id} created in dataset {dataset_id} from {gcs_uri} with auto-detected schema.")