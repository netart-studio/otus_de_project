# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости для confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librdkafka-dev \
        gcc && \
    rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY ./app/requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем основной скрипт
COPY ./app/kafka_to_minio_parquet.py .

# Указываем точку входа
ENTRYPOINT ["python", "kafka_to_minio_parquet.py"]