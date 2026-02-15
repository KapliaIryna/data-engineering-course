# Фінальний проєкт

## Що зроблено

4 пайплайни для обробки даних продажів електроніки:

1. **process_sales** - CSV дані про продажі, raw -> bronze -> silver, партиціонування по даті
2. **process_customers** - CSV дані клієнтів, дедуплікація, очистка пустих полів
3. **process_user_profiles** - JSON дані профілів, розбиття full_name на first/last name
4. **enrich_user_profiles** - MERGE в Redshift, заповнення пропущених даних (state, ім'я) з user_profiles

## Стек

- S3 - зберігання даних (raw/bronze/silver)
- Glue - ETL джоби
- Redshift Serverless - gold шар + аналітика
- Airflow на ECS - оркестрація

## Результат аналітичного запиту

> В якому штаті найбільше TV-покупок від клієнтів 20-30 років за 1-10 вересня?

**Idaho — 179 покупок**

## Що працює

- Glue джоби запускаються через Airflow (GlueJobOperator)
- Дані проходять raw -> bronze -> silver
- MERGE в gold виконано, таблиця user_profiles_enriched заповнена (47469 записів)
- Аналітичний запит працює

## Проблема

enrich_user_profiles DAG не вдалося запустити повністю через Airflow — проблема з доступом до Redshift Data API з ECS контейнера (схоже на мережеві налаштування VPC). MERGE виконано напряму через Redshift Query Editor, дані в gold є.

## Структура

```
airflow/dags/     - DAG файли
glue/             - Glue ETL скрипти
sql/              - SQL для Redshift
screenshots/      - скріни результатів
```

## Скріншоти

- Glue jobs (успішні рани)
- Airflow DAGs
- Redshift - результат запиту
- S3 структура
