FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV AIRFLOW_HOME=/opt/airflow

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY init-airflow.sh .
COPY dags /opt/airflow/dags

RUN chmod +x init-airflow.sh