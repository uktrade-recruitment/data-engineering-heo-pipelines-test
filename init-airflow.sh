#!/usr/bin/env bash
set -e

echo "waiting for postgres to be ready..."
until python -c "
import psycopg2
import sys
try:
    conn = psycopg2.connect(
        dbname='postgres',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    conn.close()
    sys.exit(0)
except psycopg2.OperationalError:
    sys.exit(1)
"; do
  echo "postgres isnt ready yet"
  sleep 2
done
echo "postgres is now ready"

echo "creating airflow db"
airflow db init || { echo "failed to create db"; exit 1; }

if ! airflow users list | grep -q "admin"; then
    echo "creating admin user"
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
else
    echo "admin user already exists so we wont create a new one"
fi

if ! airflow connections get postgres_default > /dev/null 2>&1; then
    echo "creating postgres_default connection"
    airflow connections add 'postgres_default' \
        --conn-type 'postgres' \
        --conn-host 'postgres' \
        --conn-login 'airflow' \
        --conn-password 'airflow' \
        --conn-schema 'airflow'
else
    echo "postgres_default already exists so we wont createa  new one"
fi

if ! airflow connections get jsonplaceholder_api > /dev/null 2>&1; then
    echo "create jsonplaceholder_api connection (this is for the data)"
    airflow connections add 'jsonplaceholder_api' \
        --conn-type 'http' \
        --conn-host 'https://jsonplaceholder.typicode.com' \
        --conn-extra '{}'
else
    echo "jsonplaceholder_api already exists so we won't create a new one"
fi

airflow db init
airflow connections add 'airflow_postgres' \
  --conn-type postgres \
  --conn-host postgres \
  --conn-port 5432 \
  --conn-login airflow \
  --conn-password airflow \
  --conn-schema airflow