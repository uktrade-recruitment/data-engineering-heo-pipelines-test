from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import psycopg2

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 30),
    "retries": 1,
}

dag = DAG(
    "companies_house_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

def test_postgres_connection():
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432",
        )
        print("connection to db was successful!")
        conn.close()
    except Exception as e:
        print(f"connection failed: {str(e)}")
        raise

test_db_task = PythonOperator(
    task_id="test_postgres_connection",
    python_callable=test_postgres_connection,
    dag=dag,
)

fetch_data_task = SimpleHttpOperator(
    task_id="fetch_company_data",
    http_conn_id="jsonplaceholder_api",
    endpoint="users/1",
    method="GET",
    response_filter=lambda response: response.json(),
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_company_table",
    postgres_conn_id="airflow_postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS company (
        company_number VARCHAR(20) PRIMARY KEY,
        company_name TEXT,
        company_status TEXT,
        incorporation_date DATE
    );
    """,
    dag=dag,
)

insert_data_task = PostgresOperator(
    task_id="insert_company_data",
    postgres_conn_id="airflow_postgres",
    sql="""
    INSERT INTO company (company_number, company_name, company_status, incorporation_date)
    VALUES (%(company_number)s, %(company_name)s, %(company_status)s, %(incorporation_date)s)
    ON CONFLICT (company_number) DO UPDATE SET
        company_name = EXCLUDED.company_name,
        company_status = EXCLUDED.company_status,
        incorporation_date = EXCLUDED.incorporation_date;
    """,
    parameters={
        "company_number": "{{ ti.xcom_pull(task_ids='fetch_company_data')['id'] | string }}",
        "company_name": "{{ ti.xcom_pull(task_ids='fetch_company_data')['name'] }}",
        "company_status": "{{ 'active' }}",
        "incorporation_date": "{{ '2023-01-01' }}",
    },
    dag=dag,
)

test_db_task >> create_table_task >> fetch_data_task >> insert_data_task