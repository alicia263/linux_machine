import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Time, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection settings
DB_USER = os.getenv('POSTGRES_USER', 'food_delivery_user')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'SuperSecurePwdHere')
DB_HOST = os.getenv('DB_HOST', '172.18.0.5')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'food_delivery_db')

DB_CONNECTION_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
FOOD_DELIVERY_TABLE_NAME = os.getenv('FOOD_DELIVERY_TABLE_NAME', 'food_delivery')

# Define the database model
Base = declarative_base()

class FoodDeliveryTable(Base):
    __tablename__ = FOOD_DELIVERY_TABLE_NAME
    id = Column(String, primary_key=True)
    Delivery_person_ID = Column(String)
    Delivery_person_Age = Column(Integer)
    Delivery_person_Ratings = Column(Float)
    Restaurant_latitude = Column(Float)
    Restaurant_longitude = Column(Float)
    Delivery_location_latitude = Column(Float)
    Delivery_location_longitude = Column(Float)
    Order_Date = Column(DateTime)
    Time_Orderd = Column(Time)
    Time_Order_picked = Column(Time)
    Weatherconditions = Column(String)
    Road_traffic_density = Column(String)
    Vehicle_condition = Column(Integer)
    Type_of_order = Column(String)
    Type_of_vehicle = Column(String)
    multiple_deliveries = Column(String)
    Festival = Column(String)
    City = Column(String)
    Time_taken = Column(String)

# Function to load data into the database
def load_data_to_postgres(df):
    logger.info('Connecting to database...')
    engine = create_engine(DB_CONNECTION_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    inspector = inspect(engine)
    if inspector.has_table(FoodDeliveryTable.__tablename__):
        Base.metadata.drop_all(engine, tables=[FoodDeliveryTable.__table__])
    
    Base.metadata.create_all(engine)
    logging.info('Inserting dataframe to database...')
    start = time.time()
    df.to_sql(FoodDeliveryTable.__tablename__, engine, if_exists='replace', index=False)
    logging.info(f'Putting df to postgres took {time.time()-start:.3f} s')

# Data processing functions
def convert_to_int64(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        logger.info(f"Converting column '{column}' to Int64")
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].astype('Int64')
        logger.info(f"Column '{column}' converted to Int64")
    return df

def convert_to_float(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        logger.info(f"Converting column '{column}' to float")
        df[column] = pd.to_numeric(df[column], errors='coerce')
        logger.info(f"Column '{column}' converted to float")
    return df

def convert_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        logger.info(f"Converting column '{column}' to datetime")
        df[column] = pd.to_datetime(df[column], format='%d-%m-%Y', errors='coerce')
        logger.info(f"Column '{column}' converted to datetime")
    return df

def drop_null_values(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    initial_shape = df.shape
    logger.info(f"Initial DataFrame shape: {initial_shape}")
    
    if subset:
        subset = [col for col in subset if col in df.columns]
    df = df.dropna(subset=subset)
    
    final_shape = df.shape
    logger.info(f"Final DataFrame shape: {final_shape}")
    logger.info(f"Dropped {initial_shape[0] - final_shape[0]} rows with null values")
    
    return df

# Main execution
if __name__ == "__main__":
    logger.info("Starting data processing")
    
    # Load data from URL
    url = "https://raw.githubusercontent.com/Ashraf1395/customer_retention_analytics/main/streaming_pipeline/data/train.csv"
    df = pd.read_csv(url)
    
    logger.info(f"Loaded DataFrame with columns: {df.columns.tolist()}")
    
    # Convert columns (adjust these based on your actual column names)
    logger.info("Converting column data types")
    df = convert_to_int64(df, 'Delivery_person_Age')
    df = convert_to_float(df, 'Delivery_person_Ratings')
    df = convert_to_datetime(df, 'Order_Date')
    
    # Drop null values
    logger.info("Dropping null values")
    df = drop_null_values(df)
    
    # Load data to PostgreSQL
    load_data_to_postgres(df)
    
    logger.info("Data processing and loading completed successfully.")
