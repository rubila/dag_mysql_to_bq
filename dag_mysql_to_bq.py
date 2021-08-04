
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from datetime import date, datetime, timedelta

default_args = {
        'owner': 'Rubi',
		'retries': 20,
        'retry_delay': timedelta(minutes= 1),
        'start_date': 
        datetime.combine(
        datetime.today() - timedelta(days=1),
        datetime.min.time()),
        }
        
with DAG(dag_id='dag_mysql_to_bq', default_args=default_args, schedule_interval="0 * * * *") as dag:
    t1 = MySqlToGoogleCloudStorageOperator(
        task_id= "run_to_gcs",
        sql = """select * from kubeccio.order where date(created_at) = '{{ ds }}'""",
        bucket ="temp",
        filename= "order_{{ ds }}.csv",
        schema_filename=None,
        mysql_conn_id='mysql_conn',
        google_cloud_storage_conn_id='gcs_conn',
        dag=dag
    )

    t2 = GoogleCloudStorageToBigQueryOperator(
        task_id= "gcs_to_bq",
        bucket="temp", 
        source_objects = ["order_{{ ds }}.csv"],
        destination_project_dataset_table ="practice.temp.order",
        schema_fields=
        [{'name':'order_id','type':'INTEGER','mode': 'NULLABLE'},
        {'name':'subtotal_price','type':'FLOAT','mode': 'NULLABLE'},
        {'name':'created_at','type':'TIMESTAMP','mode': 'NULLABLE'}], 
        source_format='CSV', write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bq_conn', google_cloud_storage_conn_id='gcs_conn',
        dag = dag
    )

t1>>t2