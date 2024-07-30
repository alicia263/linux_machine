import os
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Time
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import inspect


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection settings
DB_USER = os.getenv('POSTGRES_USER', 'customer_data_user')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'SuperSecurePwdHere')
DB_HOST = os.getenv('DB_HOST', '172.18.0.2')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'customer_data_db')
DB_CONNECTION_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
CUSTOMER_DELIVERY_TABLE_NAME = os.getenv('CUSTOMER_DELIVERY_TABLE_NAME', 'food_delivery')

DB_CONNECTION_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
CUSTOMER_DELIVERY_TABLE_NAME = 'customer_data'  # Update this to match your actual table name

# Define the database model
Base = declarative_base()

class CustomerDataTable(Base):
    __tablename__ = CUSTOMER_DELIVERY_TABLE_NAME 
    id = Column(Integer, primary_key=True, autoincrement=True)  # Primary key column
    Age = Column(Integer)
    Gender = Column(String)
    Marital_Status = Column(String)
    Occupation = Column(String)
    Monthly_Income = Column(String)  # Assuming it might contain non-numeric characters
    Educational_Qualifications = Column(String)
    Family_size = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    Pin_code = Column(Integer)
    Medium_P1 = Column(String)
    Medium_P2 = Column(String)
    Meal_P1 = Column(String)
    Meal_P2 = Column(String)
    Perference_P1 = Column(String)
    Perference_P2 = Column(String)
    Ease_and_convenient = Column(String)
    Time_saving = Column(String)
    More_restaurant_choices = Column(String)
    Easy_Payment_option = Column(String)
    More_Offers_and_Discount = Column(String)
    Good_Food_quality = Column(String)
    Good_Tracking_system = Column(String)
    Self_Cooking = Column(String)
    Health_Concern = Column(String)
    Late_Delivery = Column(String)
    Poor_Hygiene = Column(String)
    Bad_past_experience = Column(String)
    Unavailability = Column(String)
    Unaffordable = Column(String)
    Long_delivery_time = Column(String)
    Delay_of_delivery_person_getting_assigned = Column(String)
    Delay_of_delivery_person_picking_up_food = Column(String)
    Wrong_order_delivered = Column(String)
    Missing_item = Column(String)
    Order_placed_by_mistake = Column(String)
    Influence_of_time = Column(String)
    Order_Time = Column(String)
    Maximum_wait_time = Column(String)
    Residence_in_busy_location = Column(String)
    Google_Maps_Accuracy = Column(String)
    Good_Road_Condition = Column(String)
    Low_quantity_low_time = Column(String)
    Delivery_person_ability = Column(String)
    Influence_of_rating = Column(String)
    Less_Delivery_time = Column(String)
    High_Quality_of_package = Column(String)
    Number_of_calls = Column(String)
    Politeness = Column(String)
    Freshness = Column(String)
    Temperature = Column(String)
    Good_Taste = Column(String)
    Good_Quantity = Column(String)
    Output = Column(String)
    Reviews = Column(String)





def rename_columns(df):
    """
    Rename specific columns in the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame with columns to be renamed.

    Returns:
    pd.DataFrame: DataFrame with renamed columns.
    """
    rename_dict = {
        'Medium (P1)': 'Medium_P1',
        'Medium (P2)': 'Medium_P2',
        'Meal(P1)': 'Meal_P1',
        'Meal(P2)': 'Meal_P2',
        'Perference(P1)': 'Perference_P1',
        'Perference(P2)': 'Perference_P2'
    }
    df.rename(columns=rename_dict, inplace=True)
    return df

def replace_nil_values(df, column_name):
    """
    Replace 'NIL', 'nil', and 'Nil' with 'No comment' in the specified column of the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to be updated.
    column_name (str): The name of the column to update.

    Returns:
    pd.DataFrame: DataFrame with updated values.
    """
    replacements = {'NIL': 'No comment', 'nil': 'No comment', 'Nil': 'No comment'}
    df[column_name] = df[column_name].replace(replacements)
    return df

# Function to load data into the database
def load_data_to_postgres(df):
    logger.info('Connecting to database...')
    engine = create_engine(DB_CONNECTION_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    inspector = inspect(engine)
    if inspector.has_table(CustomerDataTable.__tablename__):
        Base.metadata.drop_all(engine, tables=[CustomerDataTable.__table__])
    
    Base.metadata.create_all(engine)
    logging.info('Inserting dataframe to database...')
    start = time.time()
    df.to_sql(CustomerDataTable.__tablename__, engine, if_exists='replace', index=False)
    logging.info(f'Putting df to postgres took {time.time()-start:.3f} s')
# def load_data_to_postgres(df):
#     """
#     Load DataFrame into PostgreSQL database.

#     Parameters:
#     df (pd.DataFrame): The DataFrame to be loaded into the database.
#     """
#     logger.info('Connecting to database...')
#     engine = create_engine(DB_CONNECTION_URL)

#     # Dynamically add columns to the table based on DataFrame columns
#     for column in df.columns:
#         if not hasattr(CustomerDataTable, column):
#             setattr(CustomerDataTable, column, Column(String))  # Use String type for simplicity

#     # Drop table if it exists and create a new one
#     if engine.dialect.has_table(engine, CustomerDataTable.__tablename__):
#         logger.info(f"Dropping existing table: {CustomerDataTable.__tablename__}")
#         Base.metadata.drop_all(engine, tables=[CustomerDataTable.__table__])

#     logger.info(f"Creating table: {CustomerDataTable.__tablename__}")
#     Base.metadata.create_all(engine)

#     logger.info('Inserting dataframe to database...')
#     start = time.time()

#     # Insert data
#     df.to_sql(CustomerDataTable.__tablename__, engine, if_exists='append', index=False)

#     logger.info(f'Inserting data to postgres took {time.time()-start:.3f} s')

# Main execution
if __name__ == "__main__":
    logger.info("Starting data processing")

    # Load data from URL
    url = "https://raw.githubusercontent.com/Ashraf1395/customer_retention_analytics/main/streaming_pipeline/data/onlinedeliverydata.csv"
    df = pd.read_csv(url)

    # Print all columns
    logger.info(f"Loaded DataFrame with columns: {df.columns.tolist()}")

    # Rename columns
    df = rename_columns(df)

    # Replace NIL values
    if 'review' in df.columns:
        df = replace_nil_values(df, 'review')

    # Load data to PostgreSQL
    load_data_to_postgres(df)

    logger.info("Data processing and loading completed successfully.")
