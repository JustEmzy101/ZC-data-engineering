import os
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from email_utils import send_pretty_email_failure, send_pretty_email_success
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ macros.ds_format(macros.ds_add(ds, -330), '%Y-%m-%d', '%Y-%m') }}.parquet"
)

# Local output file template where the downloaded parquet file will be written.
# The actual path is rendered at runtime and will live under AIRFLOW_HOME.
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ macros.ds_format(macros.ds_add(ds, -330), '%Y-%m-%d', '%Y-%m') }}.parquet")

OUTPUT_FILE_NAME = "output_{{ macros.ds_format(macros.ds_add(ds, -330), '%Y-%m-%d', '%Y-%m') }}.parquet"
OUTPUT_TABLE_NAME = "output_{{ macros.ds_format(macros.ds_add(ds, -330), '%Y-%m-%d', '%Y-%m') }}"

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'staging_dataset')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn_id')
    gcs_hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file
    )
    
    # client = storage.Client()
    # bucket = client.bucket(bucket)
    # blob = bucket.blob(object_name)
    # blob.upload_from_filename(local_file)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # start_date is the first logical execution date for the DAG (not the
    # runtime). Because catchup=False below, historical runs will not be backfilled.
    'start_date': datetime(2025, 10, 5),
    'email': ['mmzidane101@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=3),
    #'on_failure_callback': send_pretty_email_failure,
    'on_success_callback': send_pretty_email_success
}

with DAG(
    dag_id='GCP_data_ingestion',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:
    
    wget_task = BashOperator(
        task_id='wget_task',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_data/{OUTPUT_FILE_NAME}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )
    bigquery_external_table_task = BigQueryInsertJobOperator(
    task_id="bigquery_external_table_task",
    gcp_conn_id="gcp_conn_id",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{OUTPUT_TABLE_NAME}`
                OPTIONS (
                    format = 'PARQUET',
                    uris = ['gs://{BUCKET}/raw_data/{OUTPUT_FILE_NAME}']
                )
            """,
            "useLegacySql": False,
            
        }
    },
)

    wget_task >> local_to_gcs_task >> bigquery_external_table_task
    