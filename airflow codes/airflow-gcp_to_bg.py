from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define DAG parameters
dag_name = 'gcs_to_bq_example_qossay_final_task'
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 24)
}

# Define the DAG
dag = DAG(dag_name, default_args=default_args, schedule_interval=None)

# Task 3: Load transformed data into BigQuery using GoogleCloudStorageToBigQueryOperator
load_data_tasks = []
for i in range(1801 ,2001):
    task_id = f'load_data_{i}'
    source_object = f'data2/data5/file-{i}.csv.gz'
    
    load_task = GoogleCloudStorageToBigQueryOperator(
        task_id=task_id,
        bucket='qossay_bucket_test',
        source_objects=[source_object],
        destination_project_dataset_table='tidal-mason-386011.qossay_marwn_test.FinalData2',
        source_format='CSV',
        write_disposition='WRITE_APPEND',
        gcp_conn_id='my_gcp_connection',
        dag=dag
    )
    load_data_tasks.append(load_task)

# Set the dependency between tasks
for i in range(1, len(load_data_tasks)):
    load_data_tasks[i].set_upstream(load_data_tasks[i-1])
	
