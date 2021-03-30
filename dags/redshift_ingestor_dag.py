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


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "sample_movie_ratings_dag"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    S3_BKT_NAME = "airflow-d"
    S3_KEY_NAME = "movie_ratings.json"
    S3_RAW_DATA_PREFIX = "files/redshift-ingestor/raw"
    S3_PROCESSED_DATA_PREFIX = "files/redshift-ingestor/processed"
    DATA_URL_01 = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    DATA_URL_02 = "https://raw.githubusercontent.com/miztiik/sample-data/master/movie_data.json"
    ATHENA_DB = "airflow_demo_athena_db"
    ATHENA_RESULTS = "athena-results"


logger = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.WARNING)


def fetch_files():
    s3c = boto3.client("s3")
    # Get file from url
    web_data = requests.get(GlobalArgs.DATA_URL_02)
    s3c.put_object(
        Bucket=GlobalArgs.S3_BKT_NAME,
        # Key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/{os.path.basename(GlobalArgs.DATA_URL_02)}",
        # Key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/movie_ratings_{datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%s')}.json",

        # s3://yourBucket/pathToTable/<PARTITION_COLUMN_NAME>=<VALUE>/<PARTITION_COLUMN_NAME>=<VALUE>/
        Key=f"{GlobalArgs.S3_RAW_DATA_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{GlobalArgs.S3_KEY_NAME}",
        Body=web_data.content
    )
    logger.info("File uploaded to s3 successfully")
    print("File uploaded to s3 successfully")


# Setup Default Arguments for DAG
DEFAULT_ARGS = {
    'owner': 'Miztiik Automation',
    'depends_on_past': False,
    'email': ['miztiik@github'],
    'email_on_failure': False,
    'email_on_retry': False
}


CREATE_ATHENA_DATABASE_MOVIES_QUERY = """
CREATE DATABASE IF NOT EXISTS airflow_demo_athena_db
  COMMENT 'Movie Rating and Metadata'
  WITH DBPROPERTIES ('creator'='M', 'Dept.'='Automation');
"""

CREATE_ATHENA_TABLE_MOVIE_RATINGS_QUERY = """
CREATE EXTERNAL TABLE IF NOT EXISTS airflow_demo_athena_db.movies_ratings (
  `year` INT,
  `title` STRING,
  `info` STRING 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://airflow-d/files/redshift-ingestor/'
TBLPROPERTIES (
  'has_encrypted_data'='false'
); 
"""

sample_movie_record = {
    "year": 2013,
    "title": "Rush",
    "info": {
        "directors": ["Ron Howard"],
        "release_date": "2013-09-02T00:00:00Z",
        "rating": 8.3,
        "genres": [
            "Action",
            "Biography",
            "Drama",
            "Sport"
        ],
        "image_url": "http://ia.media-imdb.com/images/M/MV5BMTQyMDE0MTY0OV5BMl5BanBnXkFtZTcwMjI2OTI0OQ@@._V1_SX400_.jpg",
        "plot": "A re-creation of the merciless 1970s rivalry between Formula One rivals James Hunt and Niki Lauda.",
        "rank": 2,
        "running_time_secs": 7380,
        "actors": [
                "Daniel Bruhl",
                "Chris Hemsworth",
                "Olivia Wilde"
        ]
    }
}


redshift_ingestor_dag_01 = DAG(
    dag_id='redshift-ingestor-dag-01',
    default_args=DEFAULT_ARGS,
    description='A simple dag to ingest files from CSV to Redshift',
    concurrency=10,
    max_active_runs=1,
    # start_date=datetime.datetime.now(),
    start_date=days_ago(0),
    schedule_interval='*/10 * * * *',
    # schedule_interval=timedelta(days=1),
    # schedule_interval='@hourly'
    dagrun_timeout=timedelta(hours=2),
    tags=['redshift-ingestor-dag-01', 'miztiik_automation'],
)

pull_files_to_s3_tsk = PythonOperator(
    task_id="pull_files_to_s3_tsk",
    python_callable=fetch_files
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
    dag=redshift_ingestor_dag_01
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


# Chain Tasks in DAG
pull_files_to_s3_tsk >> check_s3_for_key_tsk >> create_athena_table_movie_ratings >> move_raw_files_to_processed_loc
create_athena_database_movie_ratings >> create_athena_table_movie_ratings
