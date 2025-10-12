from dotenv import load_dotenv
import os
from etl_project.connectors.alpaca_api import AlpacaApiClient
from etl_project.assets.assets import extract_alpaca_data, convert_to_dataframe, extract_stock_symbol, initial_transform, load, transform_and_load_analysis_table
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import (
        MetaData, inspect)
from etl_project.utilities.utilities import get_checkpoint, save_checkpoint
from etl_project.assets.assets import define_stock_bars_table
from jinja2 import Environment, FileSystemLoader
from etl_project.metadata.log_metadata import DatabaseLogger

# Logging
from loguru import logger

# Config
import yaml
from pathlib import Path

import schedule, time

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

def pipeline():
    try:
        # If table exists, perform incremental extract and load
        if inspect(postgres_sql_client.engine).has_table(source_table_name):
            logger.info(f"Table '{source_table_name}' exists in the database")
            #Logs data to the "metadata" table
            metadata_logger.insert_log(f"Table '{source_table_name}' exists in the database")

            # Get last checkpoint
            last_checkpoint = get_checkpoint(postgres_sql_client.engine, checkpoint_table_name, table_name=source_table_name)
            last_checkpoint_date = last_checkpoint[:10]
            # next_day_date = (datetime.strptime(last_checkpoint_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

            # Incremental extract from last checkpoint date to today (including last checkpoint date in case there are missing data for that date)
            logger.info(f"Last checkpoint is {last_checkpoint_date}. Performing incremental extract from {last_checkpoint_date} to today")
            #Logs data to the "metadata" table
            metadata_logger.insert_log(f"Last checkpoint is {last_checkpoint_date}. Performing incremental extract from {last_checkpoint_date} to today")

            extracted_stock_bars = extract_alpaca_data(
                stock_symbol_csv_path=config['config']['stock_symbol_relative_path'],
                start_date=last_checkpoint_date,
                timeframe="1Day",
                api_key_id=ALPACA_API_KEY_ID,
                api_secret_key=ALPACA_API_SECRET_KEY
            )
            if not extracted_stock_bars:
                logger.warning("No data returned from Alpaca API. Possibly start_date is too recent. Check start date and try again / later.")
                #Logs data to the "metadata" table
                metadata_logger.insert_log("No data returned from Alpaca API. Possibly start_date is too recent. Check start date and try again / later.")

            # Reflect existing table
            metadata = MetaData()
            metadata.reflect(bind=postgres_sql_client.engine, only=[source_table_name])
            table = metadata.tables[source_table_name]

        # else, perform full extract and load
        else:
            logger.info(f"Table '{source_table_name}' does not exist")
            logger.info("Performing initial full extract of data to create the table")
            #Logs data to the "metadata" table
            metadata_logger.insert_log(f"Table '{source_table_name}' does not exist")
            metadata_logger.insert_log("Performing initial full extract of data to create the table")


            # Full initial extract data for September 2025
            extracted_stock_bars = extract_alpaca_data(
                stock_symbol_csv_path=config['config']['stock_symbol_relative_path'],
                start_date="2025-09-01",
                end_date="2025-09-30",
                timeframe="1Day",
                api_key_id=ALPACA_API_KEY_ID,
                api_secret_key=ALPACA_API_SECRET_KEY
            )

            # Define table schema
            metadata = MetaData()
            table = define_stock_bars_table(metadata, source_table_name)

        # Convert extracted data to DataFrame
        df_stock_bars = convert_to_dataframe(extracted_stock_bars)
        logger.info(f"Extracted {df_stock_bars.shape[0]} rows of data from Alpaca API")
        metadata_logger.insert_log(f"Extracted {df_stock_bars.shape[0]} rows of data from Alpaca API")

        # Extract stock symbol data
        df_stock_symbol = extract_stock_symbol(config['config']['stock_symbol_relative_path'])

        # Transform data    
        df_stock = initial_transform(df_stock_bars, df_stock_symbol)

        # Load data to Postgres
        load(
            df=df_stock,
            postgresql_client=postgres_sql_client,
            table=table,
            metadata=metadata,
            load_method=load_method
        )
        
        logger.info(f"Complete data loading to table '{source_table_name}' using load method of {load_method}") 
        metadata_logger.insert_log(f"Complete data loading to table '{source_table_name}' using load method of {load_method}")

        # Save latest timestamp as checkpoint
        latest_timestamp = df_stock['timestamp'].max()
        save_checkpoint(postgres_sql_client.engine, checkpoint_table_name, source_table_name, latest_timestamp)
    except KeyError as e:
        logger.error(f"KeyError: {e}.")
        metadata_logger.insert_log(f"KeyError: {e}.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        metadata_logger.insert_log(f"An error occurred: {e}")

    # Transform and load to analysis table
    try:
        environment = Environment(loader=FileSystemLoader("etl_project/assets/sql/transform"))
        transform_and_load_analysis_table(environment, postgres_sql_client.engine)
        logger.info(f"Data transformation and loading to analysis table completed")
        metadata_logger.insert_log(f"Data transformation and loading to analysis table completed")

    except Exception as e:
        logger.error(f"An error occurred during analysis table transformation and loading: {e}")
        metadata_logger.insert_log(f"An error occurred during analysis table transformation and loading: {e}")

if __name__ == "__main__":
    
    config = get_yaml_config()

    # Fetching environment variables
    load_dotenv(override=True)
    ALPACA_API_KEY_ID = os.getenv("APCA-API-KEY-ID")
    ALPACA_API_SECRET_KEY = os.getenv("APCA-API-SECRET-KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    metadata_logger = DatabaseLogger(
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=SERVER_NAME,
        port=5432,
        dbname=DATABASE_NAME
    )

    logger.info("Fetching data from Alpaca Market API")
    metadata_logger.insert_log("Fetching data from Alpaca Market API")

    alpacaApiClient = AlpacaApiClient(
        api_key_id=ALPACA_API_KEY_ID,
        api_secret_key=ALPACA_API_SECRET_KEY
    )   

    logger.info("Create a connection to the database")
    metadata_logger.insert_log("Create a connection to the database")

    postgres_sql_client = PostgreSqlClient(
            DB_USERNAME, 
            DB_PASSWORD, 
            SERVER_NAME, 
            DATABASE_NAME
        )

    # Get config values
    source_table_name = config['config']['source_table_name']
    checkpoint_table_name = config['config']['checkpoint_table_name']
    load_method = config['config']['load_method']

    # Run pipeline
    pipeline()