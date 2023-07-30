from airflow.decorators import dag, task
from datetime import datetime

@task
def task1():
    # Function to execute for task1
    print("Executing task1")

@task
def task2():
    # Function to execute for task2
    print("Executing task2")

@task
def task3():
    # Function to execute for task3
    print("Executing task3")

@dag(default_args={'start_date': datetime(2023, 6, 1)})
def my_dag():
    t1 = task1()
    t2 = task2()
    t3 = task3()

    t1 >> t2 >> t3

dag = my_dag()
