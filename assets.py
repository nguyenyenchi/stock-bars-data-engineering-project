import pandas as pd
from etl_project.connectors.alpaca_api import AlpacaApiClient
from etl_project.connectors.postgresql import PostgreSqlClient

def get_stock_symbol(csv_file_path: str) -> str:
    """
    Returns a list of stock symbols to fetch data for.

    Returns:
        A string of stock symbols
    """
    stock_list = pd.read_csv(csv_file_path)["Symbol"].tolist()

    return ",".join(stock_list)

def initial_extract_alpaca_data(
        stock_symbol_csv_path: str,
        start_date: str,
        end_date: str,
        timeframe: str,
        api_key_id: str,
        api_secret_key: str
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