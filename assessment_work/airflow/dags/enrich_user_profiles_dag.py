# DAG: enrich_user_profiles (silver → gold, manual)
# MERGE customers + user_profiles → gold.user_profiles_enriched
# Заповнює пропущені first_name, last_name, state; додає birth_date, phone_number

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import time

WORKGROUP = "iryna-data-platform-workgroup"
DATABASE = "dev"
REGION = "us-east-1"

MERGE_SQL = """
MERGE INTO gold.user_profiles_enriched
USING (
    SELECT
        c.client_id,
        COALESCE(c.first_name, up.first_name) AS first_name,
        COALESCE(c.last_name, up.last_name) AS last_name,
        c.email,
        c.registration_date,
        COALESCE(c.state, up.state) AS state,
        up.birth_date,
        up.phone_number
    FROM silver.customers c
    LEFT JOIN silver.user_profiles up ON LOWER(c.email) = LOWER(up.email)
) AS source
ON gold.user_profiles_enriched.client_id = source.client_id
WHEN MATCHED THEN UPDATE SET
    first_name = source.first_name,
    last_name = source.last_name,
    email = source.email,
    registration_date = source.registration_date,
    state = source.state,
    birth_date = source.birth_date,
    phone_number = source.phone_number,
    updated_at = GETDATE()
WHEN NOT MATCHED THEN INSERT (
    client_id, first_name, last_name, email, registration_date,
    state, birth_date, phone_number, created_at, updated_at
) VALUES (
    source.client_id, source.first_name, source.last_name, source.email,
    source.registration_date, source.state, source.birth_date,
    source.phone_number, GETDATE(), GETDATE()
);
"""


def run_redshift_merge():
    """Redshift Data API: MERGE запит для збагачення даних"""
    client = boto3.client('redshift-data', region_name=REGION)

    resp = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=MERGE_SQL
    )
    stmt_id = resp['Id']
    print(f"Statement ID: {stmt_id}")

    # Очікуємо завершення
    for _ in range(60):
        status = client.describe_statement(Id=stmt_id)['Status']
        print(f"Status: {status}")
        if status == 'FINISHED':
            return
        if status == 'FAILED':
            error = client.describe_statement(Id=stmt_id).get('Error', 'Unknown')
            raise Exception(f"Query failed: {error}")
        time.sleep(5)

    raise Exception("Timeout")


with DAG(
    'enrich_user_profiles',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'enrichment', 'manual'],
) as dag:

    PythonOperator(
        task_id='merge_to_gold',
        python_callable=run_redshift_merge,
    )
