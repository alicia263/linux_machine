FROM apache/airflow:2.7.3

USER root
RUN apt-get update && apt-get install -y gcc g++ make
RUN apt-get install -y gosu


USER airflow

COPY ./airflow/requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt


RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" kafka_python==2.0.2 unidecode==1.3.7 apache-airflow-providers-docker==2.1.0

