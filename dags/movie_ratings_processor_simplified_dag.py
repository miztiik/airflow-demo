# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from io import StringIO
from io import BytesIO
from time import sleep
import csv
import requests
import json
import boto3
import zipfile
import io
import os


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "sample_movie_ratings_dag"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    S3_BKT_NAME = "airflow-d"
    S3_KEY = "files/"
    DATA_URL = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"


# s3_bucket_name = 'airflow-d'
# s3_key = 'files/'
# redshift_cluster = 'my-redshift-cluster'
# redshift_db = 'dev'
# redshift_dbuser = 'awsuser'
# redshift_table_name = 'movie_demo'
# download_http = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'


def download_zip():
    s3c = boto3.client('s3')
    # indata = requests.get(download_http)
    indata = requests.get(GlobalArgs.DATA_URL)
    n = 0
    with zipfile.ZipFile(io.BytesIO(indata.content)) as z:
        zList = z.namelist()
        print(zList)
        for i in zList:
            print(i)
            zfiledata = BytesIO(z.read(i))
            n += 1
            s3c.put_object(
                Bucket=GlobalArgs.S3_BKT_NAME,
                Key=GlobalArgs.S3_KEY+i+'/'+i, Body=zfiledata
            )


DEFAULT_ARGS = {
    'owner': 'Miztiik Automation',
    'depends_on_past': False,
    'email': ['miztiik@github'],
    'email_on_failure': False,
    'email_on_retry': False
}


with DAG(
    dag_id='movie_ratings_processor_simplified_03jan',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval='0 */4 * * *',
    tags=['get_movies_ratings', 'clean_ratings'],
) as dag:
    check_s3_for_key = S3KeySensor(
        task_id='check_s3_for_key',
        bucket_key=GlobalArgs.S3_KEY,
        wildcard_match=True,
        bucket_name=GlobalArgs.S3_BKT_NAME,
        s3_conn_id='aws_default',
        timeout=20,
        poke_interval=5,
        dag=dag
    )
    files_to_s3 = PythonOperator(
        task_id="files_to_s3",
        python_callable=download_zip
    )

    check_s3_for_key >> files_to_s3
