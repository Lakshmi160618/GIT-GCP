from google.cloud import storage
from google.cloud import bigquery
import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def bucket_creator(cloud_event):
    """Triggered by a new file upload to a Cloud Storage bucket."""
    # Define the GCS bucket and file path
    #bucket_name = data['bucket']
    #file_name = data['name']
    #print(f"New file uploaded: {file_name} to {bucket_name}")

    # Initialize the BigQuery client
    bigquery_client = bigquery.Client()

    # Define the table schema
    schema = [
        bigquery.SchemaField('cust_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('cust_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('cust_age', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('cust_address', 'STRING', mode='NULLABLE')
    ]

    # Define the BigQuery table ID
    table_id = 'ilovegcp-426017.useeastsouthcarolona.gcs_to_bq'

    # Define the GCS file URI
    uri = 'gs://kaka1/cust_info.csv'

    # Define the job configuration
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    # Load the data into the table
    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    # Print the loaded row count
    table = bigquery_client.get_table(table_id)
    print(f'Loaded {table.num_rows} rows to {table_id}')