from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'bq_bq_mohammad',
    'start_date': datetime(2023, 1, 1),
   # 'retries': 1,
    #'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bigquery_aggregations_mohammad_partition',
    default_args=default_args,
    schedule_interval=None,
)

dataset_id = 'mohammad'
table_id = 'aggregation_table_partition'
destination_dataset_table = 'tidal-mason-386011.mohammad.aggregation_table_partition'


# standardSQL
sql_query = '''
#standardSQL\nSELECT count(*)
 FROM `tidal-mason-386011.qossay_marwn_test.FinalData_partition`
 WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2023-07-12")
 and `int64_field_3` = 101
'''


create_table_task = BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    dataset_id=dataset_id,
    table_id=table_id,
    gcp_conn_id='bq_conn_mohammad',
    dag=dag,
)

aggregation_task = BigQueryExecuteQueryOperator(
    task_id='aggregation',
    sql=sql_query,
    destination_dataset_table=destination_dataset_table,
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='bq_conn_mohammad',
    use_legacy_sql=False, 
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

create_table_task >> aggregation_task >> end_task
