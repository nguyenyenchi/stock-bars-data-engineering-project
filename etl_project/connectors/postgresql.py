import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects import postgresql

class PostgreSqlClient:
    """
        A client for querying postgresql database
    """

    def __init__(self, 
                 username: str, 
                 password: str, 
                 server_name: str, 
                 database_name: str, 
                 port: int =5432):
        self.username = username
        self.password = password
        self.server_name = server_name
        self.database_name = database_name
        self.port = port

        # create connection to database
        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=self.username,
            password=self.password,
            host=self.server_name,
            port=self.port,
            database=self.database_name,
        )

        self.engine = create_engine(connection_url)

    def write_to_database(self, table: Table, metadata: MetaData, data = pd.DataFrame) -> None:

        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]

        metadata.create_all(self.engine)  # creates table if it does not exist

        # Insert values from df_exchange into table stock_price_tesla_table
        insert_statement = postgresql.insert(table).values(
            data.to_dict(orient="records")
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c
                for c in insert_statement.excluded
                if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)


    def select_all(self, table: Table) -> list[dict]:
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)




