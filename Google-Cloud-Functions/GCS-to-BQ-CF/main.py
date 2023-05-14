import os
from google.cloud import storage
from google.cloud import bigquery


def load_data_into_bq(data, context):
    """Triggered by a new file upload to a Cloud Storage bucket."""
    # Define the GCS bucket and file path
    bucket_name = data['bucket']
    file_name = data['name']
    print(f"New file uploaded: {file_name} to {bucket_name}")

    # Initialize the BigQuery client
    bigquery_client = bigquery.Client()

    # Define the table schema
    schema = [
        bigquery.SchemaField('Rating', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Company_Name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Job_Title', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Salary', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Salaries_Reported',
                             'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Location', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Employment_Status',
                             'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Job_Roles', 'STRING', mode='NULLABLE'),
    ]

    # Define the BigQuery table ID
    table_id = 'gcp-training-swamy.mydataset_new.gcs_to_bq'

    # Define the GCS file URI
    uri = f'gs://{bucket_name}/{file_name}'

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
