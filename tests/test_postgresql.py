import pytest
import os
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, Column, String, Float, Integer
from etl_project.connectors.postgresql import PostgreSqlClient

# Load environment variables from .env file
load_dotenv()

# ---------- Fixtures ----------

@pytest.fixture(scope="module")
def db_client():
    return PostgreSqlClient(
        username=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        server_name=os.getenv("SERVER_NAME", "localhost"),
        database_name=os.getenv("DATABASE_NAME"),
        port=int(os.getenv("PORT", "5432")),
    )

@pytest.fixture
def test_table_and_metadata():
    metadata = MetaData()
    table = Table(
        "stock_data_test", metadata,
        Column("stock", String, primary_key=True),
        Column("company", String),
        Column("timestamp", String, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float), 
        Column("volume", Integer),
        Column("volume_weighted_avg_price", Float),
        Column("number_of_trades", Integer),
    )
    return table, metadata

@pytest.fixture
def sample_data():
    return [
        {
            "stock": "TSLA",
            "company": "Tesla Inc",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 250.0,
            "high": 255.0,
            "low": 245.0,
            "close": 252.0,
            "volume": 1000000,
            "volume_weighted_avg_price": 251.5,
            "number_of_trades": 5000,
        },
        {
            "stock": "AAPL",
            "company": "Apple Inc",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 170.0,
            "high": 172.0,
            "low": 168.0,
            "close": 171.0,
            "volume": 800000,
            "volume_weighted_avg_price": 170.8,
            "number_of_trades": 4200,
        }
    ]

# ---------- Tests ----------

def test_connectivity(db_client):
    connection = db_client.engine.connect()
    assert connection is not None
    connection.close()

def test_insert(db_client, test_table_and_metadata, sample_data):
    table, metadata = test_table_and_metadata
    db_client.drop_table(table.name)
    db_client.insert(sample_data, table, metadata)

    results = db_client.select_all(table)
    assert len(results) == 2
    assert any(row["stock"] == "TSLA" for row in results)
    assert any(row["stock"] == "AAPL" for row in results)

def test_overwrite(db_client, test_table_and_metadata, sample_data):
    table, metadata = test_table_and_metadata
    #db_client.insert(sample_data, table, metadata)

    new_data = [
        {
            "stock": "GOOG",
            "company": "Alphabet Inc",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 2800.0,
            "high": 2820.0,
            "low": 2780.0,
            "close": 2810.0,
            "volume": 600000,
            "volume_weighted_avg_price": 2805.0,
            "number_of_trades": 3000,
        }
    ]
    db_client.overwrite(new_data, table, metadata)

    results = db_client.select_all(table)
    assert len(results) == 1
    assert results[0]["stock"] == "GOOG"

def test_upsert(db_client, test_table_and_metadata):
    table, metadata = test_table_and_metadata
    db_client.drop_table(table.name)
    db_client.create_table(metadata)

    # Insert initial data
    initial_data = [
        {
            "stock": "TSLA",
            "company": "Tesla Inc",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 250.0,
            "high": 255.0,
            "low": 245.0,
            "close": 252.0,
            "volume": 1000000,
            "volume_weighted_avg_price": 251.5,
            "number_of_trades": 5000,
        }
    ]
    db_client.upsert(initial_data, table, metadata)

    # Upsert with updated close + new row
    updated_data = [
        {
            "stock": "TSLA",
            "company": "Tesla Inc",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 250.0,
            "high": 255.0,
            "low": 245.0,
            "close": 260.0,  # Updated
            "volume": 1000000,
            "volume_weighted_avg_price": 255.0,
            "number_of_trades": 5100,
        },
        {
            "stock": "MSFT",
            "company": "Microsoft Corp",
            "timestamp": "2025-10-01T10:00:00Z",
            "open": 300.0,
            "high": 305.0,
            "low": 295.0,
            "close": 302.0,
            "volume": 700000,
            "volume_weighted_avg_price": 301.0,
            "number_of_trades": 3500,
        }
    ]
    db_client.upsert(updated_data, table, metadata)

    results = db_client.select_all(table)
    assert len(results) == 2

    tsla = next(row for row in results if row["stock"] == "TSLA")
    msft = next(row for row in results if row["stock"] == "MSFT")

    assert tsla["close"] == 260.0
    assert msft["company"] == "Microsoft Corp"
