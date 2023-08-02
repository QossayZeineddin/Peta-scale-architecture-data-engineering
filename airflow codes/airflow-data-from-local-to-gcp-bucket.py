import os
import random
import gzip
import string
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

BUCKET_NAME = "qossay_bucket_test"  # Update with your GCS bucket name
LOCAL_DIR = "/home/qossay_zeineddin/test_generate/data10"  # Update with your local directory path

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def generate_file():

    for i in range(3601,4001):
        filename = f"file-{i}.csv.gz"
        filepath = os.path.join(LOCAL_DIR, filename)

        # Upload the compressed file to GCS
        gcs_hook = GoogleCloudStorageHook()
        destination_blob = f"data2/data10/{filename}"  # Destination path in GCS
        gcs_hook.upload(BUCKET_NAME, destination_blob, filepath, mime_type="application/gzip")

        # Remove the local files
        #os.remove(filepath)
        #os.remove(compressed_filepath)
        #content = ""

dag = DAG(
    "local_machen_to_bucket",
    default_args=DEFAULT_ARGS,
    schedule_interval=None  # One-time execution
)

generate_files_task = PythonOperator(
    task_id="generate_files",
    python_callable=generate_file,
    dag=dag
)
