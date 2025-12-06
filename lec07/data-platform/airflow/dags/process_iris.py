from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import timedelta

from python_scripts.train_model import process_iris_data


DBT_PROJECT_PATH = "/opt/airflow/dags/dbt/homework"
DBT_PROFILES_DIR = "/opt/airflow/dags/dbt" 
EMAIL_RECIPIENT = "my.email@mail.com" 


# аргументи DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CRON_SCHEDULE = "0 22 * * *"  # 22:00 UTC = 01:00 Київ (GMT+3)
START_DATE = pendulum.datetime(2025, 4, 22, tz="UTC")
END_DATE = pendulum.datetime(2025, 4, 24, 23, 59, 59, tz="UTC")  # включно з 24.04


# визначення DAG
with DAG(
    dag_id="process_iris",
    default_args=default_args,
    description="Пайплайн обробки даних Iris та навчання ML класифікатора",
    start_date=START_DATE, 
    end_date=END_DATE,
    schedule=CRON_SCHEDULE, 
    catchup=True, 
    tags=["dbt", "ml", "iris"],
) as dag:
    
    execution_date_var = "{{ ds }}"
    
    # 0. dbt-Dependencies: Встановлення пакетів
    dbt_deps_install = BashOperator(
        task_id="dbt_deps_install",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )
    
    # 1. dbt-Seed: завантаження CSV
    load_seed_data = BashOperator(
        task_id="dbt_seed_iris",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt seed --profiles-dir {DBT_PROFILES_DIR}",
    )

    
    # 2. dbt-Трансформація
    transform_data = BashOperator(
        task_id="dbt_transform_iris",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt run --project-dir {DBT_PROJECT_PATH} --models +mart.iris_processed --vars '{{\"execution_date\": \"{execution_date_var}\"}}' --profiles-dir {DBT_PROFILES_DIR}",
    )

    # 3. навчання ML моделі 
    train_model_task = PythonOperator(
        task_id="train_ml_model",
        python_callable=process_iris_data,
        op_kwargs={
            "execution_date": execution_date_var
        }
    )

    # 3. e-mail про успішне виконання
    send_success_email = EmailOperator(
        task_id="send_success_email",
        to=EMAIL_RECIPIENT,
        subject=f"Пайплайн 'process_iris' успішнозавершено для {{{{ ds }}}}",
        html_content=f"""
            <h3>Пайплайн 'process_iris' для дати {{{{ ds }}}} успішно завершено!</h3>
        """,
    )

    # порядок виконання тасок
    dbt_deps_install >> load_seed_data >> transform_data >> train_model_task >> send_success_email