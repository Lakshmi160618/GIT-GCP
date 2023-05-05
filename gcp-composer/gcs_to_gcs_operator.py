from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Variable
from datetime import datetime, timedelta

source_bucket = Variable.get('source_bucket')
destination_bucket =Variable.get('destination_bucket')
source_object =Variable.get('source_object')
destination_object =Variable.get('destination_object')

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'gcs_to_gcs_variable_dag',
    default_args=default_args,
    description='Load data from one GCS bucket to another using variables',
    schedule_interval=timedelta(days=1)
)



transfer_files = GCSToGCSOperator(
    task_id='transfer_file',
    source_bucket=source_bucket,
    destination_bucket=destination_bucket,
    source_object=[source_object],
    destination_object=destination_object,
    dag=dag
)

transfer_files