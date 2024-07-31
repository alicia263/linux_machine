import os
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import from_json, col
from db_utils import prepare_db
import sqlalchemy
from datetime import datetime
from typing import List, Any
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.automap import automap_base

FOOD_DELIVERY_TABLE_NAME = os.getenv("FOOD_DELIVERY_TABLE_NAME", "food_delivery")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv('POSTGRES_DB', 'food_delivery_db')
POSTGRES_JDBC_CONNECTION_URL = os.getenv(
    "POSTGRES_JDBC_CONNECTION_URL",
    f"jdbc:postgresql://postgres:{POSTGRES_PORT}/{DB_NAME}",
)
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer_data")
SPARK_STREAM_CHECKPOINTS_PATH = os.getenv(
    "SPARK_STREAM_CHECKPOINTS_PATH", "/home/airflow/spark_streaming_checkpoints"
)
POSTGRES_PROPERTIES = {
    "user": "food_delivery_user",
    "password": "SuperSecurePwdHere",
    "driver": "org.postgresql.Driver",
}


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("PySpark to Postgres")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
    print("Created a Spark session successfully")
    return spark


def create_df_from_kafka(spark_session) -> DataFrame:
    df = (
        spark_session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    return df


def process_df(df) -> DataFrame:
    """
    Process the input DataFrame by defining a schema and converting the DataFrame to match this schema.
    
    Args:
        df (DataFrame): The input DataFrame.
    
    Returns:
        DataFrame: The DataFrame with the schema applied.
    """
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("Delivery_person_ID", StringType(), True),
            StructField("Delivery_person_Age", IntegerType(), True),
            StructField("Delivery_person_Ratings", FloatType(), True),
            StructField("Restaurant_latitude", FloatType(), True),
            StructField("Restaurant_longitude", FloatType(), True),
            StructField("Delivery_location_latitude", FloatType(), True),
            StructField("Delivery_location_longitude", FloatType(), True),
            StructField("Order_Date", DateType(), True),
            StructField("Time_Orderd", StringType(), True),
            StructField("Time_Order_picked", StringType(), True),
            StructField("Weatherconditions", StringType(), True),
            StructField("Road_traffic_density", StringType(), True),
            StructField("Vehicle_condition", IntegerType(), True),
            StructField("Type_of_order", StringType(), True),
            StructField("Type_of_vehicle", StringType(), True),
            StructField("multiple_deliveries", StringType(), True),
            StructField("Festival", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Time_taken", StringType(), True),
        ]
    )

    processed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return processed_df


def write_df_to_db(processed_df):
    query = (
        processed_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: (
                batch_df.write.jdbc(
                    POSTGRES_JDBC_CONNECTION_URL,
                    FOOD_DELIVERY_TABLE_NAME,
                    "append",
                    properties=POSTGRES_PROPERTIES,
                )
            )
        )
        .trigger(once=True)
        .option(
            "checkpointLocation", SPARK_STREAM_CHECKPOINTS_PATH
        )  # set checkpoints so pyspark won't reread the same messages
        .start()
    )
    return query.awaitTermination()


def stream_kafka_to_db():
    prepare_db()  # create table if not exist
    print("Creating Spark session with Kafka and Postgres packages")
    spark = create_spark_session()
    df = create_df_from_kafka(spark)
    processed_df = process_df(df)
    print("Processed Spark DataFrame")
    write_df_to_db(processed_df)
    print("Successfully streamed Kafka to Postgres with Spark Streaming!")


if __name__ == "__main__":
    stream_kafka_to_db()
