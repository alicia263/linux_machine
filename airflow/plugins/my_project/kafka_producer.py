import os
import time
import logging
import numpy as np
import pandas as pd
import json
from datetime import datetime
from kafka import KafkaProducer

# Create a logger to handle application logs
logger = logging.getLogger("kafka_producer.py")
logger.setLevel(logging.INFO)
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(log_format)
logger.addHandler(ch)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer_data")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092")

def preprocess_input_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the input DataFrame by converting column names to lowercase and sorting by 'order_date'.
    
    Args:
        df (pd.DataFrame): The DataFrame to preprocess.
    
    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """
    # Convert all column names into lower case for simplicity
    df.columns = map(lambda x: x.lower(), df.columns)
    # Sort DataFrame by 'order_date' and reset index
    df = df.sort_values("order_date", ascending=True).reset_index(drop=True)
    return df

def convert_to_int64(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Convert a specified column in the DataFrame to Int64 type.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the column.
        column (str): The name of the column to convert.
    
    Returns:
        pd.DataFrame: The DataFrame with the column converted to Int64.
    """
    if column in df.columns:
        logger.info(f"Converting column '{column}' to Int64")
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].astype('Int64')
        logger.info(f"Column '{column}' converted to Int64")
    return df

def convert_to_float(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Convert a specified column in the DataFrame to float type.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the column.
        column (str): The name of the column to convert.
    
    Returns:
        pd.DataFrame: The DataFrame with the column converted to float.
    """
    if column in df.columns:
        logger.info(f"Converting column '{column}' to float")
        df[column] = pd.to_numeric(df[column], errors='coerce')
        logger.info(f"Column '{column}' converted to float")
    return df

def convert_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Convert a specified column in the DataFrame to datetime type.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the column.
        column (str): The name of the column to convert.
    
    Returns:
        pd.DataFrame: The DataFrame with the column converted to datetime.
    """
    if column in df.columns:
        logger.info(f"Converting column '{column}' to datetime")
        df[column] = pd.to_datetime(df[column], format='%d-%m-%Y', errors='coerce')
        logger.info(f"Column '{column}' converted to datetime")
    return df

def drop_null_values(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """
    Drop rows from the DataFrame that contain null values in specified columns.
    
    Args:
        df (pd.DataFrame): The DataFrame to process.
        subset (list, optional): List of columns to check for null values. If None, all columns are checked.
    
    Returns:
        pd.DataFrame: The DataFrame with rows containing null values removed.
    """
    initial_shape = df.shape
    logger.info(f"Initial DataFrame shape: {initial_shape}")
    
    if subset:
        subset = [col for col in subset if col in df.columns]
    df = df.dropna(subset=subset)
    
    final_shape = df.shape
    logger.info(f"Final DataFrame shape: {final_shape}")
    logger.info(f"Dropped {initial_shape[0] - final_shape[0]} rows with null values")
    
    return df

def create_kafka_producer() -> KafkaProducer:
    """
    Create and configure a KafkaProducer instance.
    
    Returns:
        KafkaProducer: The configured KafkaProducer instance.
    """
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

def stream_data(producer: KafkaProducer, data: pd.DataFrame) -> None:
    """
    Stream data from the DataFrame to a Kafka topic.
    
    Args:
        producer (KafkaProducer): The KafkaProducer instance used to send data.
        data (pd.DataFrame): The DataFrame containing the data to stream.
    """
    logger.info("Start streaming data!")
    pointer = 0
    while True:
        if pointer == len(data):
            pointer = 0

        # Change date to today to imitate real-time API
        row = data.iloc[pointer]
        row["order_date"] = datetime.now().strftime("%Y-%m-%d")
        record = json.loads(row.to_json())
        logger.info(f"Sent: {record}")
        producer.send(topic=KAFKA_TOPIC, value=record)
        producer.flush()

        pointer += 1
        time.sleep(10)  # For demo and development
        # time.sleep(60) # Uncomment to stream data every minute

# # Main execution
# if __name__ == "__main__":
#     logger.info("Reading a small subset of data for dummy streaming...")
#     food_delivery_stream = pd.read_csv("https://raw.githubusercontent.com/Ashraf1395/customer_retention_analytics/main/streaming_pipeline/data/train.csv")
#     food_delivery_stream = preprocess_input_df(food_delivery_stream)
    
#     producer = create_kafka_producer()
#     stream_data(producer, food_delivery_stream)

def main() -> None:
    """
    Main function to read data, preprocess it, create a Kafka producer, and stream the data.
    """
    logger.info("Reading a small subset of data for dummy streaming...")
    
    # Read the data from the provided URL
    food_delivery_stream = pd.read_csv("https://raw.githubusercontent.com/Ashraf1395/customer_retention_analytics/main/streaming_pipeline/data/train.csv")
    
    # Preprocess the data
    food_delivery_stream = preprocess_input_df(food_delivery_stream)
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    # Stream the data to Kafka
    stream_data(producer, food_delivery_stream)

# Main execution
if __name__ == "__main__":
    main()

