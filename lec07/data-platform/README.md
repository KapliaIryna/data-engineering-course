# Apache Airflow ML Pipeline

Створення ML pipeline в Apache Airflow для обробки датасету Iris з використанням dbt для трансформацій та scikit-learn для тренування моделі.

## Структура DAG

DAG `process_iris` складається з 5 послідовних тасок:

```
dbt_deps_install → dbt_seed_iris → dbt_transform_iris → train_ml_model → send_success_email
```

### Таски:

1. **dbt_deps_install** - Встановлення dbt пакетів
2. **dbt_seed_iris** - Завантаження iris датасету в PostgreSQL через dbt seed
3. **dbt_transform_iris** - Трансформація даних
4. **train_ml_model** - Тренування Random Forest моделі та збереження метрик
5. **send_success_email** - Відправка email про успішне виконання

## Конфігурація

### Розклад

- **Cron**: `"0 22 * * *"` (щодня о 22:00 UTC = 01:00 Київ)
- **Start date**: 2025-04-22
- **End date**: 2025-04-24
- **Catchup**: True (виконано 3 runs)

### Email notifications

- **SMTP**: Mailhog (localhost:1025)
- **Web UI**: http://localhost:8025

## Файли проекту

```
lec07/data-platform/
├── README.md
├── docker-compose.yaml
├── .env
├── airflow/
│   └── dags/
│       ├── process_iris.py
│       ├── python_scripts/
│       │   └── train_model.py
│       └── dbt/
│           ├── profiles.yml
│           └── homework/
│               ├── dbt_project.yml
│               ├── packages.yml
│               ├── models/
│               │   ├── staging/
│               │   │   └── stg_iris.sql
│               │   └── mart/
│               │       └── iris_processed.sql
│               ├── macros/
│               │   └── log_transform_postgres.sql
│               └── seeds/
│                   └── iris_dataset.csv
├── postgres_analytics/
└── postgres_airflow/
```

## Результати виконання

**3 успішні runs**:

- 2025-04-22T22:00:00+00:00
- 2025-04-23T22:00:00+00:00
- 2025-04-24T22:00:00+00:00

**Всі 5 тасок виконані успішно** (див. screenshots/):

1. dbt_deps_install
2. dbt_seed_iris
3. dbt_transform_iris
4. train_ml_model
5. send_success_email

**Email нотифікації відправлені** через Mailhog
