from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pandas as pd

default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 4, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def bq_hook():
    # Create a BigQueryHook instance
    bq_hook = BigQueryHook(gcp_conn_id='bigquery_default')

    # Define the SQL query to execute
    sql = """
        SELECT *
        FROM data-warehouse-99.airflow_demo6.master_table;
    """
    job_id = bq_hook.run_query(sql,
                                destination_dataset_table='data-warehouse-99.gcp_training.target_table',
                                write_disposition='WRITE_EMPTY',
                                allow_large_results=False,
                                flatten_results=None, udf_config=None,
                                use_legacy_sql=False,
                                maximum_billing_tier=None,
                                maximum_bytes_billed=None,
                                create_disposition='CREATE_IF_NEEDED',
                                query_params=None,
                                labels=None,
                                schema_update_options=None,
                                priority='INTERACTIVE',
                                time_partitioning=None,
                                api_resource_configs=None,
                                cluster_fields=None,
                                location=None,
                                encryption_configuration=None)


dag = DAG(
    dag_id='BigQueryHooks',
    default_args=default_args,
    schedule_interval=None
)

bq_hook = PythonOperator(
    task_id='run_query',
    python_callable=bq_hook,
    dag=dag
)

trigger = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='cloudsql_mysql_conector_dag',
    dag=dag
)


bq_hook >> trigger 
