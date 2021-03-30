import logging
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def add_nos(n_1=10, n_2=20):
    logging.info(f'{{"sum":{n_1 + n_2}}}')


def sub_nos(n_1=10, n_2=20):
    logging.info(f'{{"sum":{n_1 - n_2}}}')


def square_no(n_1=10):
    logging.info(f'{{"sum":{n_1 * n_1}}}')


# Setup Default Arguments for DAG
DEFAULT_ARGS = {
    'owner': 'Miztiik Automation',
    'depends_on_past': False,
    'email': ['miztiik@github'],
    'email_on_failure': False,
    'email_on_retry': False
}


math_dag = DAG(
    dag_id='sample-math-dag-01',
    default_args=DEFAULT_ARGS,
    description='A simple math tutorial DAG',
    # start_date=days_ago(1),
    start_date=datetime.datetime.now(),


    schedule_interval='*/10 * * * *',
    # schedule_interval=timedelta(days=1),
    # schedule_interval='@hourly'
    dagrun_timeout=timedelta(hours=2),
    tags=['sample-math-dag', 'miztiik_automation'],
)


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = PythonOperator(
    task_id="addition_task",
    python_callable=add_nos,
    dag=math_dag
)

t2 = PythonOperator(
    task_id="subtraction_task",
    python_callable=sub_nos,
    depends_on_past=False,
    retries=3,
    dag=math_dag
)

square_task = PythonOperator(
    task_id="square_task",
    python_callable=square_no,
    depends_on_past=True,
    retries=3,
    dag=math_dag
)

math_dag.doc_md = __doc__

t1.doc_md = """\
#### Addition Task Documentation
A simple task to add two numbers
![miztiik-success-green](https://img.shields.io/badge/Miztiik:Automation:Airflow:Level-300-blue)
"""

# Configure Task Dependencies
t1 >> t2
t1 >> square_task
