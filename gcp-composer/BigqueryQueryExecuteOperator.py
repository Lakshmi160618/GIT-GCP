from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
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
    'sql_job111',
    default_args=default_args,
    description='Load data from one GCS bucket to another using variables',
    schedule_interval=timedelta(days=1)
)

sql='LOAD DATA OVERWRITE mydataset.gcstobigquery_table FROM FILES (format = "CSV", uris = ["gs://uday_project1/source.csv"])',
 
bigquery_execute_multi_query = BigQueryExecuteQueryOperator(
    task_id="execute_multi_query",
    sql=[
        f'LOAD DATA OVERWRITE mydataset.gcstobigquery_table FROM FILES (format = "CSV", uris = ["gs://uday_project1/source.csv"])',
        f"SELECT COUNT(*) FROM mydataset.gcstobigquery_table",
    ],
    use_legacy_sql=False,
    location='us',
	dag=dag
)
bigquery_execute_multi_query
