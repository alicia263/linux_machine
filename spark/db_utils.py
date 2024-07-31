import os
import sqlalchemy
from datetime import datetime
from typing import List, Any
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.automap import automap_base

FOOD_DELIVERY_TABLE_NAME = os.getenv("FOOD_DELIVERY_TABLE_NAME", "food_delivery")
FORECAST_TABLE_NAME = os.getenv("FORECAST_TABLE_NAME", "forecast_results")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_CONNECTION_URL = os.getenv(
    "DB_CONNECTION_URL",
    f"postgresql://food_delivery_user:SuperSecurePwdHere@postgres:{POSTGRES_PORT}/food_delivery_db",
)



Base = declarative_base()

class FoodDeliveryTable(Base):
    __tablename__ = FOOD_DELIVERY_TABLE_NAME
    id = Column(String, primary_key=True)
    Delivery_person_ID = Column(String)
    Delivery_person_Age = Column(Integer)
    Delivery_person_Ratings = Column(Float)  # Updated from float to Float
    Restaurant_latitude = Column(Float)      # Updated from float to Float
    Restaurant_longitude = Column(Float)     # Updated from float to Float
    Delivery_location_latitude = Column(Float) # Updated from float to Float
    Delivery_location_longitude = Column(Float) # Updated from float to Float
    Order_Date = Column(DateTime)
    Time_Orderd = Column(Time)  # Correct type for time
    Time_Order_picked = Column(Time)  # Correct type for time
    Weatherconditions = Column(String)
    Road_traffic_density = Column(String)
    Vehicle_condition = Column(Integer)
    Type_of_order = Column(String)
    Type_of_vehicle = Column(String)
    multiple_deliveries = Column(String)
    Festival = Column(String)
    City = Column(String)
    Time_taken = Column(String)

def prepare_db() -> None:
    print("Preparing database tables...")
    engine = create_engine(DB_CONNECTION_URL)
    Base.metadata.create_all(engine)
    print("Done database preparation")

def open_db_session(engine: sqlalchemy.engine) -> sqlalchemy.orm.Session:
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def unique_list_from_col(
    session: sqlalchemy.orm.Session, table, column: str
) -> List[Any]:
    unique_rets = session.query(getattr(table, column)).distinct().all()
    unique_list = [ret[0] for ret in unique_rets]
    return unique_list

def get_table_from_engine(engine, table_name):
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    table_obj = getattr(Base.classes, table_name)
    return table_obj
