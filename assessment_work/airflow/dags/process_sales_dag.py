# DAG: process_sales (raw → bronze → silver)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

PROJECT = "iryna-data-platform"
BUCKET = "iryna-data-platform-data-lake-730335603706"

with DAG(
    'process_sales',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 9, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'etl'],
) as dag:

    GlueJobOperator(
        task_id='glue_process_sales',
        job_name=f"{PROJECT}-process_sales",
        region_name='us-east-1',
        script_args={
            '--database_name': 'iryna_data_platform_database',
            '--data_lake_bucket': BUCKET,
        },
        wait_for_completion=True,
    )
