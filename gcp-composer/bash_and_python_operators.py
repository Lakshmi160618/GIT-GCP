# importing section
import json
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from datetime import timedelta
from airflow.models import Variable
from google.cloud import bigquery

# define dictionary object
# variable section
# Define the bash command to execute
json_schema = '/tmp/schema/local-schema-file.json'
project_id = 'modern-replica-387105'
dataset_id = 'mydataset'
table_id = 'mytable' 

# default arguments secction
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
def python_function2(): 
	import datetime
	dataset_id = 'mydataset'
	from google.cloud import bigquery
	client = bigquery.Client()
	project = client.project
	dataset_ref = bigquery.DatasetReference(project, dataset_id)
	table_ref = dataset_ref.table('mytable')
	table = client.get_table(table_ref)  # API request
	
	assert table.expires is None
	
	# set table to expire 5 days from now
	expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
		days=10
	)
	table.expires = expiration
	table = client.update_table(table, ["expires"])  # API request
	
	# expiration is stored in milliseconds
	margin = datetime.timedelta(microseconds=1000)
	assert expiration - margin <= table.expires <= expiration + margin

def python_function(): 
	print("I am printing something from this python function only")
	python_function2()


#Dag instantiation section
dag = DAG(
    'python_and_bash_examples_dag',
    default_args=default_args,
    description='checking dags',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.

# tasks definitions section
bq_ls = BashOperator(
    task_id='bq_ls',
    bash_command='bq ls mydataset',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)

bq_extract = BashOperator(
    task_id='bq_extract',
    bash_command= 'bq extract --compression=GZIP --destination_format=CSV --field_delimiter=tab --print_header=false mydataset.mytable gs://uday_project1/myFile1.csv.gzip ',
    dag=dag,
    depends_on_past=False, 
    do_xcom_push=False
)
 
python_task = PythonOperator(
    task_id='run_query',
    python_callable=python_function,
    dag=dag
)

bq_extract >> bq_ls >> python_task

