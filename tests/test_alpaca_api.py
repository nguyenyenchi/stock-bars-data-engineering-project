import os
import pytest
from dotenv import load_dotenv
from etl_project.connectors.alpaca_api import AlpacaApiClient 

load_dotenv()

def test_alpaca_api():
    api_key = os.getenv("APCA-API-KEY-ID")
    api_secret = os.getenv("APCA-API-SECRET-KEY")

    if not api_key or not api_secret:
        pytest.skip("Alpaca API keys not found in environment variables")

    client = AlpacaApiClient(api_key, api_secret)

    symbols = "AAPL"
    timeframe = "1Day"
    start_time = "2023-01-01"
    end_time = "2023-01-05"

    result = client.get_alpaca_api_data(symbols, timeframe, start_time, end_time)

    # Basic checks
    assert isinstance(result, dict)
    assert symbols in result
    assert isinstance(result[symbols], list)
    assert len(result[symbols]) > 0 
