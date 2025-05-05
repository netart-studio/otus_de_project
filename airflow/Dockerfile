# Базовый образ Airflow
FROM apache/airflow:2.10.3-python3.12
#FROM apache/airflow:slim-3.0.0-python3.12

USER root

# Установка зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    librdkafka-dev && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

USER airflow

# Установка Python пакетов
COPY ./airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование DAG-файлов
#COPY ./airflow/dags /usr/local/airflow/dags

# Выполнение миграций базы данных Airflow
#RUN airflow db migrate

