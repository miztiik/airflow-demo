import logging
import datetime
from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.contrib.operators.s3_copy_object_operator import S3CopyObjectOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

from airflow.utils.dates import days_ago

from time import sleep

import requests
import json
import boto3


class GlobalArgs():
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    DEMO_SUFFIX = "v2"
    S3_BKT_NAME = "airflow-d"
    S3_KEY_NAME = "movie_ratings.json"
    DATA_URL_01 = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    DATA_URL_02 = "https://raw.githubusercontent.com/miztiik/sample-data/master/movie_data.json"
    S3_RAW_DATA_PREFIX = f"files/athena_ingestor_{DEMO_SUFFIX}/raw"
    S3_PROCESSED_DATA_PREFIX = f"files/athena_ingestor_{DEMO_SUFFIX}/processed"
    ATHENA_DB = f"airflow_demo_athena_db_{DEMO_SUFFIX}"
    ATHENA_TABLE_NAME = f"movies_ratings_{DEMO_SUFFIX}"
    ATHENA_RESULTS = f"athena_results_{DEMO_SUFFIX}"
    REDSHIFT_IAM_ROLE_ARN="arn:aws:iam::111122223333:role/airflow-demo-redshift-sta-redshiftClusterRole4D302-16TCUYLEG7D09"
    REDSHIFT_CLUSTER_ID=f"airflow-demo-redshift-stack-redshiftcluster-1y63tdvhck5n"
    REDSHIFT_CLUSTER_DB="comments_cluster"
    REDSHIFT_CLUSTER_USER="airflow_user"
    REDSHIFT_TABLE_NAME="airflow_demo_movies"




logger = logging.getLogger(__name__)
logging.getLogger("boto3").setLevel(logging.WARNING)


def fetch_files(**kwargs):
    ti = kwargs["task_instance"]
    s3c = boto3.client("s3")
    # Get file from url
    web_data = requests.get(GlobalArgs.DATA_URL_02)
    # moovy_file_key =f"{GlobalArgs.S3_RAW_DATA_PREFIX}/{os.path.basename(GlobalArgs.DATA_URL_02)}",
    # moovy_file_key =f"{GlobalArgs.S3_RAW_DATA_PREFIX}/movie_ratings_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%s')}.json",
    # moovy_file_key = s3://yourBucket/pathToTable/<PARTITION_COLUMN_NAME>=<VALUE>/<PARTITION_COLUMN_NAME>=<VALUE>/
    moovy_file_key = f"{GlobalArgs.S3_RAW_DATA_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{GlobalArgs.S3_KEY_NAME}"
    s3c.put_object(
        Bucket=GlobalArgs.S3_BKT_NAME,
        Key=moovy_file_key,
        Body=web_data.content
    )
    logger.info("File uploaded to s3 successfully")
    print("File uploaded to s3 successfully")
    ti.xcom_push(key="moovy_file_key", value=moovy_file_key)
    return {"moovy_file_key":moovy_file_key}


def create_redshift_table():
    rsd = boto3.client("redshift-data")
    resp = rsd.execute_statement(
        ClusterIdentifier=GlobalArgs.REDSHIFT_CLUSTER_ID,
        Database=GlobalArgs.REDSHIFT_CLUSTER_DB,
        DbUser=GlobalArgs.REDSHIFT_CLUSTER_USER,
        Sql=f"CREATE TABLE IF NOT EXISTS {GlobalArgs.REDSHIFT_TABLE_NAME} (title character varying, year int,info varchar(max));"
    )
    logger.info("Redshift table created successfully")
    logger.info(resp)
    return "Ok"

def s3_to_redshift(**kwargs):
    ti = kwargs["task_instance"]
    moovy_file_key = ti.xcom_pull(key="moovy_file_key", task_ids="pull_files_to_s3_tsk")
    print(moovy_file_key)
    sqlQuery=f"COPY {GlobalArgs.REDSHIFT_TABLE_NAME} FROM 's3://{GlobalArgs.S3_BKT_NAME}/{moovy_file_key}' IAM_ROLE '{GlobalArgs.REDSHIFT_IAM_ROLE_ARN}' json 'auto ignorecase';"
    print(sqlQuery)
    rsd = boto3.client('redshift-data')
    resp = rsd.execute_statement(
        ClusterIdentifier=GlobalArgs.REDSHIFT_CLUSTER_ID,
        Database=GlobalArgs.REDSHIFT_CLUSTER_DB,
        DbUser=GlobalArgs.REDSHIFT_CLUSTER_USER,
        Sql=sqlQuery
    )
    logger.info("Redshift Insert successfully")
    logger.info(resp)
    return "Ok"

# Setup Default Arguments for DAG
DEFAULT_ARGS = {
    'owner': 'Miztiik Automation',
    'depends_on_past': False,
    'email': ['miztiik@github'],
    'email_on_failure': False,
    'email_on_retry': False
}


CREATE_ATHENA_DATABASE_MOVIES_QUERY = (
 f" CREATE DATABASE IF NOT EXISTS {GlobalArgs.ATHENA_DB}"
 f" COMMENT 'Movie Rating and Metadata'"
 f" WITH DBPROPERTIES ('creator'='M', 'Dept.'='Automation');"
)

CREATE_ATHENA_TABLE_MOVIE_RATINGS_QUERY = (
 f"CREATE EXTERNAL TABLE IF NOT EXISTS {GlobalArgs.ATHENA_DB}.{GlobalArgs.ATHENA_TABLE_NAME} ("
 f"  `year` INT,"
 f"  `title` STRING,"
 f"  `info` STRING "
 f")"
 f" ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'"
 f" WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')"
 f" LOCATION 's3://{GlobalArgs.S3_BKT_NAME}/{GlobalArgs.S3_RAW_DATA_PREFIX}/'"
 f" TBLPROPERTIES ('has_encrypted_data'='false');"
)



redshift_ingestor_dag = DAG(
    dag_id=f"athena_redshift_ingestor_dag_{GlobalArgs.DEMO_SUFFIX}",
    default_args=DEFAULT_ARGS,
    description='A simple dag to ingest JSON files to Athena & Redshift',
    concurrency=10,
    max_active_runs=1,
    # start_date=datetime.datetime.now(),
    start_date=days_ago(0),
    schedule_interval='*/12 * * * *',
    # schedule_interval=timedelta(days=1),
    # schedule_interval='@hourly'
    dagrun_timeout=timedelta(hours=2),
        tags=[f'athena_redshift_ingestor_dag_{GlobalArgs.DEMO_SUFFIX}', 'miztiik_automation'],
)

pull_files_to_s3_tsk = PythonOperator(
    task_id="pull_files_to_s3_tsk",
    python_callable=fetch_files,
    provide_context=True
)

check_s3_for_key_tsk = S3KeySensor(
    task_id='check_s3_for_key',
    depends_on_past=False,
    timeout=20,
    poke_interval=5,
    soft_fail=True,
    # bucket_key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/movie_ratings_*",
    bucket_key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{GlobalArgs.S3_KEY_NAME}",
    bucket_name=GlobalArgs.S3_BKT_NAME,
    wildcard_match=True,
    s3_conn_id='aws_default',
    dag=redshift_ingestor_dag
)

# Task to create Athena Database
create_athena_database_movie_ratings = AWSAthenaOperator(
    task_id="create_athena_database_movie_ratings",
    query=CREATE_ATHENA_DATABASE_MOVIES_QUERY,
    database=GlobalArgs.ATHENA_DB,
    output_location=f"s3://{GlobalArgs.S3_BKT_NAME}/{GlobalArgs.ATHENA_RESULTS}/create_athena_database_movie_ratings"
)


# Task to create Athena Table
create_athena_table_movie_ratings = AWSAthenaOperator(
    task_id="create_athena_table_movie_ratings",
    query=CREATE_ATHENA_TABLE_MOVIE_RATINGS_QUERY,
    database=GlobalArgs.ATHENA_DB,
    output_location=f"s3://{GlobalArgs.S3_BKT_NAME}/{GlobalArgs.ATHENA_RESULTS}/create_athena_table_movie_ratings"
)

# Task to move processed file
move_raw_files_to_processed_loc = S3CopyObjectOperator(
    task_id="move_raw_files_to_processed_loc",
    source_bucket_key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{GlobalArgs.S3_KEY_NAME}",
    dest_bucket_key=f"{GlobalArgs.S3_PROCESSED_DATA_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{GlobalArgs.S3_KEY_NAME}",
    source_bucket_name=GlobalArgs.S3_BKT_NAME,
    dest_bucket_name=GlobalArgs.S3_BKT_NAME,
    wildcard_match=True,
    aws_conn_id='aws_default'
)

#
create_redshift_table_if_not_exists = PythonOperator(
        task_id="create_redshift_table_if_not_exists",
        python_callable=create_redshift_table
    )

transfer_to_redshift = PythonOperator(
        task_id="transfer_to_redshift",
        python_callable=s3_to_redshift,
        provide_context=True
    )


# Chain Tasks in DAG
pull_files_to_s3_tsk >> check_s3_for_key_tsk >> create_athena_table_movie_ratings >> move_raw_files_to_processed_loc
create_athena_database_movie_ratings >> create_athena_table_movie_ratings

pull_files_to_s3_tsk >> create_redshift_table_if_not_exists >> transfer_to_redshift



