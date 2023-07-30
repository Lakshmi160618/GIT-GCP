from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'gcs_to_bigquery_operator',
    default_args=default_args,
    description='Load data from one GCS bucket to another using variables',
    schedule_interval=timedelta(days=1)
)

load_data_task = GoogleCloudStorageToBigQueryOperator(
    task_id='load_data_task',
    bucket='uday_project1',
    source_objects=['source.csv'],
    destination_project_dataset_table='modern-replica-387105.mydataset.gcstobigquery_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING'},
        {'name': 'post_abbr', 'type': 'STRING'} 
        # Add more schema fields as needed
    ],
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    autodetect=False,
    skip_leading_rows=1,
    dag=dag
)

load_data_task
