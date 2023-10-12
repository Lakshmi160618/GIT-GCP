from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import xml.etree.ElementTree as ET
import json
from google.cloud import storage

# Define your XML processing function
def process_xml_and_upload_to_gcs():
    bucket_name = 'fourthbucket123'
    input_blob_name = 'ampledata.xml'
    output_blob_name = 'output/json_file.json'
    
    # Initialize a GCS client
    client = storage.Client()

    # Get the GCS bucket
    bucket = client.get_bucket(bucket_name)

    # Get the input blob (XML file) from the GCS bucket
    input_blob = bucket.get_blob(input_blob_name)

    if input_blob is not None:
        xml_data = input_blob.download_as_text()

        root = ET.fromstring(xml_data)

        customers = []

        for customer_elem in root.findall('customer'):
            customer = {
                'id': customer_elem.find('id').text,
                'name': customer_elem.find('name').text,
                'email': customer_elem.find('email').text,
                'phone': customer_elem.find('phone').text
            }
            customers.append(customer)

        # Convert the data to JSON
        json_data = json.dumps({'customers': customers})

        # Upload the JSON data to GCS
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_string(json_data, content_type='application/json')

# Define your DAG
default_args = {
    'start_date': datetime(2023, 10, 12),
    'retries': 1,
}

dag = DAG('xml_processing_dag_to_bq1', default_args=default_args, schedule_interval=None)

# Task to process XML and convert to JSON
process_task = PythonOperator(
    task_id='process_xml_and_upload_to_gcs',
    python_callable=process_xml_and_upload_to_gcs,
    dag=dag,
)

# Task to load JSON data into BigQuery
load_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='load_json_to_bq',
    bucket='fourthbucket123',  # Replace with your GCS bucket name
    source_objects=['output/json_file.json'],  # Adjust the path as needed
    destination_project_dataset_table='dev-project-401115.bq_to_gcs.airflowtable',
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    skip_leading_rows=1,
    field_delimiter=',',
    dag=dag,
)

process_task >> load_to_bq
