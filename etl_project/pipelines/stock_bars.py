from dotenv import load_dotenv
import os
from etl_project.connectors.alpaca_api import AlpacaApiClient
from etl_project.assets.assets import initial_extract_alpaca_data, convert_to_dataframe, extract_stock_symbol, transform, initial_load
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import (
        Table,
        Column,
        Integer,
        String,
        MetaData,
        Float)

# Logging
from loguru import logger

# Config
import yaml
from pathlib import Path

def get_yaml_config():
    yaml_file_path = __file__.replace(".py", ".yaml") # file rename to get .yaml file
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
            return config
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file."
        )

if __name__ == "__main__":
    
    # logger.info("Get config variables")
    config = get_yaml_config() # Get config (Create one config file per pipeline)

    logger.info("Fetching environment variables")
    load_dotenv(override=True)
    ALPACA_API_KEY_ID = os.getenv("APCA-API-KEY-ID")
    ALPACA_API_SECRET_KEY = os.getenv("APCA-API-SECRET-KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    logger.info("Fetching data from Alpaca Market API")
    alpacaApiClient = AlpacaApiClient(
        api_key_id=ALPACA_API_KEY_ID,
        api_secret_key=ALPACA_API_SECRET_KEY
    )   

    # Extract data for September 2025
    extracted_stock_bars = initial_extract_alpaca_data(
        stock_symbol_csv_path=config['config']['stock_symbol_relative_path'],
        start_date="2025-09-01",
        end_date="2025-09-30",
        timeframe="1Day",
        api_key_id=ALPACA_API_KEY_ID,
        api_secret_key=ALPACA_API_SECRET_KEY
    )

    df_stock_bars = convert_to_dataframe(extracted_stock_bars)

    # Extract stock symbol data
    df_stock_symbol = extract_stock_symbol(config['config']['stock_symbol_relative_path'])

    # Transform data    
    df_stock = transform(df_stock_bars, df_stock_symbol)

    # Create a connection to the database
    postgres_sql_client = PostgreSqlClient(DB_USERNAME, DB_PASSWORD, SERVER_NAME, DATABASE_NAME)

    metadata = MetaData()
    table = Table(
        "stock_bars",
        metadata,
        Column("stock", String, primary_key=True),
        Column("company", String),
        Column("timestamp", String, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float), 
        Column("volume", Integer),
        Column("volume_weighted_avg_price", Float),
        Column("number_of_trades", Integer)
    )

    print(df_stock.shape)

    # Initial load of df_stock into table (with metadata) using postgresql_client connection
    initial_load(
        df_stock=df_stock, 
        postgresql_client=postgres_sql_client,
        table=table, 
        metadata=metadata)
    logger.info(f"{len(df_stock)} rows loaded to the database") 
