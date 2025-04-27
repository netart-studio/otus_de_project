FROM python:3.12-slim


# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY app/ .

WORKDIR /app

CMD ["python", "app.py"]