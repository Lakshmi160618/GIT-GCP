from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mysqlhook_dag',
    default_args=default_args,
    description='A simple MySQLHook DAG',
    schedule_interval=timedelta(days=1),
)

def query_mysql():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_hook')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM kingdome")
    results = cursor.fetchall()
    print(results)
    conn.close()

with dag:
    t1 = PythonOperator(
        task_id='mysql_query_task',
        python_callable=query_mysql,
    )