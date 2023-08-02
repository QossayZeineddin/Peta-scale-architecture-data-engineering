from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'external_table_asmaa',
    'start_date': days_ago(1),
}

dag = DAG(
    'bigquery_aggregations_external_asmaa',
    default_args=default_args,
    schedule_interval=None,
)

dataset_id = 'my_dataset'
table_id = 'external_aggregation_table_asmaa'


# standardSQL
sql_query = '''
#standardSQL\nSELECT EXTRACT(MONTH FROM timestamp_field_0) AS month,
       AVG(int64_field_3) AS average_data
FROM `tidal-mason-386011.my_dataset.asmaa-external-new` where int64_field_2 = 50
GROUP BY month 
ORDER BY month ASC
'''

aggregation_task = BigQueryExecuteQueryOperator(
    task_id='aggregation',
    sql=sql_query,
    destination_dataset_table="tidal-mason-386011.my_dataset.external_aggregation_table_asmaa",
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='bq_conn_mohammad',
    use_legacy_sql=False, 
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

aggregation_task >> end_task
