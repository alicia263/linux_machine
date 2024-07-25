from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from src.scripts.postgres import (
subtract_date,
    date_from_baseline_back,
    read_csv,
    process_data,
    connect_to_db,
    setup_database,
    insert_data_to_db,
)
import os

start_date = datetime.today() - timedelta(days=1)

default_args = {
    "owner": "Ajay",
    "depends_on_past": False,
    "email": ["junioralexio607@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="batch_processing_dag",
    default_args=default_args,
    description="Batch processing of the data",
    schedule_interval=timedelta(weeks=1),  # run weekly
    start_date=datetime.now(),
    catchup=False,
    tags=["batch_processing", "postgres"],
) as dag:

    # Date transformation tasks
    date_transformation_task1 = PythonOperator(
        task_id="date_transformation_task1",
        python_callable=subtract_date,
        provide_context=True,  # Access TaskInstance ('ti')
    )

    date_transformation_task2 = PythonOperator(
        task_id="date_transformation_task2",
        python_callable=date_from_baseline_back,
        provide_context=True,
    )

    # Reading data task
    read_the_data_task = PythonOperator(
        task_id="read_the_data_task",
        python_callable=read_csv,
        provide_context=True,
        op_kwargs={'file_path': 'datasets/rossmann-store-sales/train_exclude_last_10d.csv'},
    )

    # Data processing task
    data_processing_task = PythonOperator(
        task_id="data_processing_task",
        python_callable=process_data,
        provide_context=True,
    )

    # Connect to database task
    connect_to_db_task = PythonOperator(
        task_id="connect_to_db_task",
        python_callable=connect_to_db,
        provide_context=True,
        op_kwargs={'db_url': os.getenv('DB_CONNECTION_URL')},
    )

    # Database setup task
    database_setup_task = PythonOperator(
        task_id="database_setup_task",
        python_callable=setup_database,
        provide_context=True,
    )

    # Insert data to database task
    insert_data_to_db_task = PythonOperator(
        task_id="insert_data_to_db_task",
        python_callable=insert_data_to_db,
        provide_context=True,
        op_kwargs={'table_name': os.getenv('SALES_TABLE_NAME')},
    )

    # Define the task dependencies
    date_transformation_task1 >> date_transformation_task2 >> read_the_data_task >> data_processing_task >> connect_to_db_task >> database_setup_task >> insert_data_to_db_task
