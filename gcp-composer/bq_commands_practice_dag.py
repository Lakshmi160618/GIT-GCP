# importing section
import json
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.models import Variable

# define dictionary object
# variable section
# Define the bash command to execute
json_schema = '/tmp/schema/local-schema-file.json'
project_id = 'data-warehouse-99'
dataset_id = 'gcp_training'
table_id = 'master_table' 

# default arguments secction
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# python callable functions section
'''This will run the DAG on the first minute of every hour, plus an additional time at the 1st minute mark of each hour.'''
#Dag instantiation section
dag = DAG(
    'bq_commands_practice_dag',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='1 1/1 * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.

# tasks definitions section
bq_ls = BashOperator(
    task_id='bq_ls',
    bash_command='bq ls ',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)


copy_json_schema = BashOperator(
    task_id='copy_json_schema',
    bash_command='gsutil cp gs://kalyan22/jsonschemafile.json /tmp/schema/local-schema-file.json',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)

create_dataset = BashOperator(
    task_id='create_dataset',
    bash_command= f'bq --location=US mk --dataset {project_id}:{dataset_id}',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)

bq_update = BashOperator(
    task_id='bq_update',
    bash_command= f'bq update --default_table_expiration 3600 {project_id}:{dataset_id}',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)

create_table = BashOperator(
    task_id='create_table',
    bash_command= f'bq mk -t {project_id}:{dataset_id}.{table_id} {json_schema}',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)
    
load_csv_to_bq = BashOperator(
    task_id='load_csv_to_bq',
    bash_command= f'bq --location=us load --skip_leading_rows=1 --source_format=CSV {project_id}:{dataset_id}.{table_id} gs://kalyan22/master_data.csv {json_schema}',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)

bq_query = BashOperator(
    task_id='display_records',
    bash_command=f"bq query --use_legacy_sql=false 'SELECT COUNT(*) as num_rows FROM `{project_id}.{dataset_id}.{table_id}`'",
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False) 

bq_head = BashOperator(
    task_id='bq_head',
    bash_command=f"bq head --max_rows=5 --start_row=3 --selected_fields=FN,MN,LN {dataset_id}.{table_id}",
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False) 
	
bq_extract = BashOperator(
    task_id='bq_extract',
    bash_command=f"bq extract --destination_format=CSV --field_delimiter=',' {dataset_id}.{table_id} gs://kalyan22/myFile1.csv",
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False) 

# tasks dependencies section

bq_ls >> copy_json_schema >> create_dataset >> bq_update >> create_table >> load_csv_to_bq >> bq_query >> bq_head >> bq_extract
