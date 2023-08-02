from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'external_table_asmaa',
    'start_date': days_ago(1),
}

dag = DAG(
    'external_new_asmaa',
    default_args=default_args,
        description='Load data from GCS to BQ external table',
    schedule_interval=None,
)

bq_dataset_id = 'my_dataset'
bq_table_id = 'asmaa-external-new'
bucket_name = 'qossay_bucket_test'
gcs_path = 'data/data8/*'
bq_project_id = 'tidal-mason-386011'

create_external_table_query = f"""
CREATE EXTERNAL TABLE `{bq_project_id}.{bq_dataset_id}.{bq_table_id}`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{bucket_name}/{gcs_path}'],
  skip_leading_rows = 0
)
"""

create_external_table_task = BigQueryExecuteQueryOperator(
    task_id='create_external_table',
    sql=create_external_table_query,
    use_legacy_sql=False,
    dag=dag,
)

create_external_table_task
