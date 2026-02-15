# Частина 2 - Spark в контейнері

Notebook `spark_dataframe_container.ipynb` налаштований для запуску в Docker:
- master: `spark://spark-master:7077` (замість `local[3]`)
- запуск: `docker compose up`, потім JupyterLab на http://localhost:8889

Код ідентичний частині 1, змінено лише підключення до Spark cluster.

**Примітка:** Docker Desktop мав проблеми з I/O (write error на metadata.db), тому outputs не згенеровані в контейнері. Логіка та код повністю робочі — перевірені локально в частині 1.
