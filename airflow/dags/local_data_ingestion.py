import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from email_utils import send_pretty_email_failure,send_pretty_email_success
from ingest_script import ingest_callable
from airflow import macros

# -----------------------------------------------------------------------------
# This DAG downloads a monthly Yellow Taxi parquet file to local storage and
# then ingests it into a Postgres table using a Python callable defined in
# `ingest_script.ingest_callable`.
#
# The file is intentionally small and straightforward so it can be used as a
# local-development example. Sections below are organized and commented:
#  - imports
#  - constants and templates (URL and output path)
#  - Postgres connection environment variables
#  - default_args for the DAG
#  - DAG context defining tasks (BashOperator + PythonOperator)
# -----------------------------------------------------------------------------


# Base path where Airflow runtime files live. Default follows common container path
# but can be overridden by the AIRFLOW_HOME environment variable on the host.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Remote data source configuration
# - URL_PREFIX: base path for the dataset
# - URL_TEMPLATE: templated URL for the file we want to download. It uses
#   Airflow macros to compute a month string based on the DAG run date (`ds`).
#   The macros.ds_add(ds, -120) and ds_format logic here appear to shift the
#   date by 120 days before formatting as YYYY-MM; adjust if your date logic
#   should be different.
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Example fixed URL (unused) kept for reference during development
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ data_interval_start.strftime(\"%Y-%m\") }}.parquet"
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ data_interval_start.strftime(\"%Y-%m\") }}.parquet"

# Template for the remote file to download. This is a Jinja-templated string
# that Airflow will render when the BashOperator runs. It uses `macros` to
# compute the target month relative to the DAG run's `ds` value.
URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ macros.ds_format(macros.ds_add(ds, -120), '%Y-%m-%d', '%Y-%m') }}.parquet"
)

# Local output file template where the downloaded parquet file will be written.
# The actual path is rendered at runtime and will live under AIRFLOW_HOME.
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ macros.ds_format(macros.ds_add(ds, -120), '%Y-%m-%d', '%Y-%m') }}.parquet"
)



# Postgres connection information is read from environment variables. This
# keeps secrets out of the DAG code and allows different deployments to
# provide credentials via Docker Compose, Kubernetes Secrets, or Airflow
# Connections depending on your setup.
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


# Default args applied to all tasks in the DAG. These control retry behavior,
# start date, owner metadata, and notifications. In production you might set
# email_on_failure and provide callbacks to notify on errors.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # start_date is the first logical execution date for the DAG (not the
    # runtime). Because catchup=False below, historical runs will not be backfilled.
    'start_date': datetime(2025, 10, 4),
    'email': ['mmzidane101@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=3),
    # Optional callbacks (commented out): these could hook into your pretty
    # email utilities to send formatted success/failure notifications.
    #'on_failure_callback': send_pretty_email_failure,
    #'on_success_callback': send_pretty_email_success
}

# Define the DAG using a context manager. The `schedule` is daily and
# catchup=False prevents backfilling runs for previous dates.
with DAG(
    dag_id='testing_local_data_ingestion',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    # Task 1: download the parquet file using curl. The bash_command uses the
    # templated URL and writes the file to the templated output path.
    wget_task = BashOperator(
        task_id='wget_task',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
    )

    # Task 2: run the ingestion Python callable. The callable `ingest_callable`
    # is responsible for reading the Parquet file and writing rows into the
    # Postgres table. We pass connection parameters via op_kwargs; in a more
    # secure setup you'd retrieve these from Airflow Connections or a Secret
    # backend instead of raw environment variables.
    ingest_task = PythonOperator(
        task_id='ingest_task',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name='yellow_taxi_data',
            parquet_file=OUTPUT_FILE_TEMPLATE,
        ),
    )

    # Define task ordering: download must finish before ingestion starts.
    wget_task >> ingest_task