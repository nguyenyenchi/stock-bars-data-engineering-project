import pandas as pd
from etl_project.connectors.alpaca_api import AlpacaApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from typing import Optional
from sqlalchemy import (
        Table, MetaData)
from sqlalchemy.engine import Engine
from sqlalchemy import (
        Table,
        Column,
        Integer,
        String,
        MetaData,
        Float)

def get_stock_symbol(csv_file_path: str) -> str:
    """
    Returns a list of stock symbols to fetch data for.

    Returns:
        A string of stock symbols
    """
    stock_list = pd.read_csv(csv_file_path)["Symbol"].tolist()

    return ",".join(stock_list)

def extract_alpaca_data(
        stock_symbol_csv_path: str,
        start_date: str,
        timeframe: str,
        api_key_id: str,
        api_secret_key: str,
        end_date: Optional[str] = None
    ) -> dict:
    """
    Extracts stock data from Alpaca Market API for a list of stock symbols.
    
    """

    alpacaApiClient = AlpacaApiClient(
        api_key_id=api_key_id,
        api_secret_key=api_secret_key
    )   

    result = alpacaApiClient.get_alpaca_api_data(
        symbols=get_stock_symbol(stock_symbol_csv_path),
        timeframe=timeframe,
        start_time=start_date,
        end_time=end_date)

    return result

def convert_to_dataframe(data: dict) -> pd.DataFrame:
    """
    Converts the extracted data to a pandas DataFrame.
    """
    # Flatten the data and add a 'symbol' column
    all_rows = []
    for symbol, records in data.items():
        for record in records:
            record['symbol'] = symbol
            all_rows.append(record)

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    return df

def extract_stock_symbol(csv_path):
    return pd.read_csv(csv_path)

def transform(df: pd.DataFrame, df_stock_symbol: pd.DataFrame) -> pd.DataFrame:
    
    # Convert timestamp column to datetime
    df['t'] = pd.to_datetime(df['t'])
    
    # rename columns to more meaningful names
    df_renamed = df.rename(
        columns={
            "c": "close",
            "h": "high",
            "l": "low",
            "n": "number_of_trades",
            "o": "open",
            "t": "timestamp",
            "v": "volume",
            "vw": "volume_weighted_avg_price",
            "symbol": "stock"
        }
    )

    df_merged = (
        pd.merge(
            left=df_renamed,
            right=df_stock_symbol,
            left_on="stock",
            right_on="Symbol",
        )
        .drop(columns=["Exchange", "Symbol"])
        .rename(columns={"Company": "company"})
    )

    # Reorder columns
    df_stock = df_merged[
        ["stock", "company", "timestamp", "open", "high", "low", "close", "volume", "volume_weighted_avg_price", "number_of_trades"]
    ]

    return df_stock

def initial_load(
    df_stock: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table, 
    metadata
):

    postgresql_client.write_to_database(table=table, metadata=metadata, data=df_stock
    )

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )

# Extract data given a sql query and engine
def extract_from_query(sql_query: str, engine: Engine) -> list[dict]: # Output format to store the data in memory. pandas might be heavy bc you need to import library
    return [dict(row) for row in engine.execute(sql_query).all()]


def define_stock_bars_table(metadata: MetaData, source_table_name: str) -> Table:
    return Table(
        source_table_name,
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
