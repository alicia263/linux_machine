

import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from db_tables import Base, RossmanSalesTable

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

SALES_TABLE_NAME = os.getenv('SALES_TABLE_NAME', 'rossman_sales')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_CONNECTION_URL = os.getenv('DB_CONNECTION_URL', f'postgresql://spark_user:SuperSecurePwdHere@172.18.0.4:{POSTGRES_PORT}/spark_pg_db')


def subtract_date(baseline_date, this_date):
    """
    Calculate the difference in days between two dates.

    Args:
        baseline_date (str): The baseline date in 'YYYY-MM-DD' format.
        this_date (str): The date to compare in 'YYYY-MM-DD' format.

    Returns:
        int: The number of days between the baseline date and the given date.
    """
    base = datetime.strptime(baseline_date, '%Y-%m-%d')
    current = datetime.strptime(this_date, '%Y-%m-%d')
    return (base - current).days


def date_from_baseline_back(baseline, n_days):
    """
    Calculate the date that is a certain number of days before the baseline date.

    Args:
        baseline (datetime): The baseline date.
        n_days (int): The number of days to subtract from the baseline date.

    Returns:
        str: The new date in 'YYYY-MM-DD' format.
    """
    return (baseline - timedelta(days=n_days)).strftime('%Y-%m-%d')


def read_csv(file_path):
    """
    Read data from a CSV file.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: The pandas DataFrame containing the CSV data.
    """
    logging.info('Reading data from csv...')
    return pd.read_csv(file_path)


def process_data(df):
    """
    Process the data to convert dates to relative dates and add dummy product names.

    Args:
        df (DataFrame): The pandas DataFrame to process.

    Returns:
        DataFrame: The processed DataFrame.
    """
    logging.info('Processing data...')
    
    ori_cols_order = df.columns
    
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    
    df['Month'] = df['Date'].apply(lambda x: x[:x.rfind('-')])
    df_sort = df.sort_values('Date', ascending=True)
    last_months = df_sort['Month'].unique()[-5:]
    last_months_df = df_sort[df_sort['Month'].isin(last_months)]
    
    latest_day = last_months_df.iloc[-1]['Date']
    last_months_df['days_from_latest'] = last_months_df['Date'].apply(lambda x: subtract_date(latest_day, x))
    last_months_df['Relative date'] = last_months_df['days_from_latest'].apply(lambda x: date_from_baseline_back(yesterday, x))
    
    last_months_df = last_months_df.drop(['Date', 'days_from_latest', 'Month'], axis=1)
    last_months_df = last_months_df.rename(columns={'Relative date': 'Date'})
    
    last_months_df['ProductName'] = "product_A"
    last_months_df = last_months_df[list(ori_cols_order) + ["ProductName"]]
    last_months_df.columns = map(lambda x: x.lower(), last_months_df.columns)
    
    return last_months_df


def connect_to_db(db_url):
    """
    Create a connection to the PostgreSQL database.

    Args:
        db_url (str): The database connection URL.

    Returns:
        Engine: The SQLAlchemy engine connected to the database.
    """
    logging.info('Connecting to database...')
    return create_engine(db_url)


def setup_database(engine):
    """
    Set up the database by dropping and creating the sales table.

    Args:
        engine (Engine): The SQLAlchemy engine connected to the database.
    """
    if engine.has_table(RossmanSalesTable.__tablename__):
        Base.metadata.drop_all(engine, tables=[RossmanSalesTable.__table__])
    Base.metadata.create_all(engine)


def insert_data_to_db(df, table_name, engine):
    """
    Insert data into the PostgreSQL database.

    Args:
        df (DataFrame): The pandas DataFrame to insert.
        table_name (str): The name of the table to insert data into.
        engine (Engine): The SQLAlchemy engine connected to the database.
    """
    logging.info('Inserting dataframe to database...')
    start = time.time()
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logging.info(f'Putting df to postgres took {time.time()-start:.3f} s')


def main():
    """
    Main function to run the batch processing pipeline.
    """
    df = read_csv('../datasets/rossmann-store-sales/train_exclude_last_10d.csv')
    processed_df = process_data(df)
    engine = connect_to_db(DB_CONNECTION_URL)
    setup_database(engine)
    insert_data_to_db(processed_df, SALES_TABLE_NAME, engine)


if __name__ == "__main__":
    main()