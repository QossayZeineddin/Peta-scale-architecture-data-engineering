import os
import random
import gzip
import string
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

BUCKET_NAME = "qossay_bucket_test"  # Update with your GCS bucket name
LOCAL_DIR = "/home/qossay_zeineddin/files"  # Update with your local directory path

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

def generate_file():
    target_filesize = 80 * 1024 * 1024  # 1 MB in bytes
    row_size = 30  # Estimated size per row in bytes
    num_rows_per_loop = 7000

    for i in range(1,101):
        filename = f"file-{i}.csv"
        filepath = os.path.join(LOCAL_DIR, filename)
        content = ""
        file_size = 0
        while file_size < target_filesize:
            # Generate file content with random data
            for _ in range(num_rows_per_loop):
                timestamp = random.randint(
                    int(datetime(2020, 1, 1).timestamp()),
                    int(datetime(2023, 12, 31).timestamp())
                )
                timestamp = datetime.fromtimestamp(timestamp).isoformat()
                geo_id = random.randint(1, 10)
                sensor_id = random.randint(1, 100)
                value = random.randint(100, 200)
                sensor_type = random.choice(["a", "b", "c"])
                row = f"{timestamp},{geo_id},{sensor_id},{value},{sensor_type}\n"
                content += row

            # Write content to a CSV file
            with open(filepath, "a") as file:
                file.write(content)

            content = ""
            # Calculate the file size
            file_size = os.path.getsize(filepath)
          
 
        # Compress the file with GZIP
        compressed_filepath = filepath + ".gz"
        with open(filepath, "rb") as file_in:
            with gzip.open(compressed_filepath, "wb") as fileout:
                fileout.write(file_in.read())

        # Upload the compressed file to GCS
        gcs_hook = GoogleCloudStorageHook()
        destination_blob = f"data/data1/{filename}.gz"  # Destination path in GCS
        gcs_hook.upload(BUCKET_NAME, destination_blob, compressed_filepath, mime_type="application/gzip")

        # Remove the local files
        os.remove(filepath)
        os.remove(compressed_filepath)
        content = ""

dag = DAG(
    "data_generation",
    default_args=DEFAULT_ARGS,
    schedule_interval=None  # One-time execution
)

generate_files_task = PythonOperator(
    task_id="generate_files",
    python_callable=generate_file,
    dag=dag
)

