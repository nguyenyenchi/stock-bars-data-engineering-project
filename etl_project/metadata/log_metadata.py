from sqlalchemy import create_engine, Table, Column, MetaData, DateTime, Text
from sqlalchemy import inspect
from sqlalchemy.sql import insert
from datetime import datetime


class DatabaseLogger:
    def __init__(self, username, password, host, port, dbname):
        self.engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}",
            echo=False  # Set to True for SQL logging
        )

        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self._define_metadata_table()
        self._create_metadata_table_if_not_exists()

    def _define_metadata_table(self):
        self.metadata_table = Table(
            'metadata', self.metadata,
            Column('timestamp', DateTime, nullable=False),
            Column('log_message', Text, nullable=False),
        )

    def _create_metadata_table_if_not_exists(self):
        inspector = inspect(self.engine)
        if 'metadata' not in inspector.get_table_names():
            self.metadata.create_all(self.engine)
            print("Table 'metadata' created.")
        else:
            print("Table 'metadata' already exists.")

    def insert_log(self, message: str):
        now = datetime.now()
        stmt = insert(self.metadata_table).values(timestamp=now, log_message=message)
        self.connection.execute(stmt)
        print(f"Inserted log: {now} - {message}")

    def close(self):
        self.connection.close()
        print("Database connection closed.")
