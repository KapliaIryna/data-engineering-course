# DAG: process_user_profiles (raw → silver, manual)
# Після успішного завершення запускає enrich_user_profiles

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = "iryna-data-platform"
BUCKET = "iryna-data-platform-data-lake-730335603706"

with DAG(
    'process_user_profiles',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 8, 1),
    schedule_interval=None,
    catchup=False,
    tags=['user_profiles', 'etl', 'manual'],
) as dag:

    glue_job = GlueJobOperator(
        task_id='glue_process_user_profiles',
        job_name=f"{PROJECT}-process_user_profiles",
        region_name='us-east-1',
        script_args={
            '--database_name': 'iryna_data_platform_database',
            '--data_lake_bucket': BUCKET,
        },
        wait_for_completion=True,
    )

    trigger_enrich = TriggerDagRunOperator(
        task_id='trigger_enrich',
        trigger_dag_id='enrich_user_profiles',
        wait_for_completion=False,
    )

    glue_job >> trigger_enrich
