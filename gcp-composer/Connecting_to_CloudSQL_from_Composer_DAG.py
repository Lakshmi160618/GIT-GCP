from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import Variable
import pymysql
import pandas as pd

def get_mysql_config():
    host = Variable.get("host")
    user = Variable.get("mysql_user")
    password = Variable.get("mysql_password")
    database = Variable.get("mysql_database")
    port = Variable.get("mysql_port")
    return {"host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port}

def my_function():
    print('Hello, Connecting to Cloud-SQL for MySql instance!')
    # Connect to the Cloud SQL instance
    conn = get_mysql_config() 
    connection = pymysql.connect(
         host=conn['host'],  # this is the cloud-sql public ip address
         user=conn['user'],
         password=conn['password'],
         database=conn['database'],
         port=int(conn['port']) # port number should be an valid integer value
         )

     # Perform database operations
    try:
          with connection.cursor() as cursor:
              # Execute SQL query
              sql = "select * from Customer;"
              cursor.execute(sql)

              # Fetch all rows and print them
              rows = cursor.fetchall()
              for row in rows:
                  print(row)
              # Fetch all rows and create a DataFrame 
              df = pd.DataFrame(list(rows), columns=[i[0] for i in cursor.description])
              print(df)
    finally:
          # Close the database connection
          connection.close()


dag = DAG('cloudsql_mysql_conector_dag',
            description='Simple DAG with a Python Operator',
            schedule_interval=None, 
            start_date=datetime(2023, 4, 27))

CloudSQL_Connect = PythonOperator(
    task_id='CloudSQL_Connect',
    python_callable=my_function,
    dag=dag
)

CloudSQL_Connect