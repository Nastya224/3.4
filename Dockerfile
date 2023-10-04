FROM apache/airflow:latest

# Установка clickhouse-driver
RUN pip install clickhouse-driver