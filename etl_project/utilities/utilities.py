from sqlalchemy import MetaData
from etl_project.assets.assets import extract_from_query
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import (
        Table, MetaData, inspect, Column, String)
from typing import Optional

def get_checkpoint(engine, checkpoint_table_name: str, table_name: str) -> Optional[str]:
    """Get last timestamp for incremental extract"""
    
    sql_query = f"""
    SELECT latest_timestamp 
    FROM {checkpoint_table_name} 
    WHERE table_name = '{table_name}';
    """
    
    latest_timestamp = extract_from_query(
        sql_query=sql_query, 
        engine=engine
    )
    
    return latest_timestamp[0].get('latest_timestamp') if latest_timestamp else None
    
def save_checkpoint(engine, checkpoint_table_name: str, table_name: str, latest_timestamp: str):
    """
    Save latest timestamp for incremental extract using SQLAlchemy upsert.
    """
    
    metadata = MetaData(bind=engine)

    # Create table if it doesn't exist
    if not inspect(engine).has_table(checkpoint_table_name):
        table = Table(
            checkpoint_table_name,
            metadata,
            Column("table_name", String, primary_key=True),   # fixed ID for checkpoint row
            Column("latest_timestamp", String)
        )
        metadata.create_all(engine)

    # Raw SQL upsert
    sql = f"""
    INSERT INTO {checkpoint_table_name} (table_name, latest_timestamp)
    VALUES (%s, %s)
    ON CONFLICT (table_name)
    DO UPDATE SET latest_timestamp = EXCLUDED.latest_timestamp;
    """

    engine.execute(sql, (table_name, latest_timestamp))



