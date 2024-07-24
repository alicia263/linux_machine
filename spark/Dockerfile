FROM bitnami/spark:3.5.0

WORKDIR /opt/bitnami/spark

# Install necessary build tools
USER root
RUN apt-get update && apt-get install -y gcc g++ make

# Switch back to the original user
USER 1001

RUN pip install py4j
RUN pip install SQLAlchemy==1.4.49 numpy==1.25.2 pandas==1.4.2

COPY ./airflow/dags/spark_streaming.py ./spark_streaming.py
COPY ./airflow/dags/db_utils.py ./db_utils.py

ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
