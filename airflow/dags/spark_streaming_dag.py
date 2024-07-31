from airflow import DAG
from airflow.settings import AIRFLOW_HOME
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import sys

from my_project.kafka_producer import main




# Set up logging
logger = logging.getLogger("kafka_producer_dag")
logger.setLevel(logging.INFO)
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(log_format)
logger.addHandler(ch)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer_data")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "host.docker.internal:9092")

start_date = datetime.today() - timedelta(days=1)


default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,  # number of retries before failing the task
    "retry_delay": timedelta(seconds=5),
}


with DAG(
    dag_id="spark_processing_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:



    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="airflow-spark-operator:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_streaming.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
        network_mode="airflow-kafka",
        dag=dag,
    )


    spark_stream_task


    
