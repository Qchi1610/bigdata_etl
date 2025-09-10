import os
# from dotenv import load_dotenv
# load_dotenv()
import pyarrow as pa 
from postgresql_client import PostgresqlClient

def create_schema():
    pstg = PostgresqlClient(
        db = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
    )

    pstg.execute_query("""CREATE SCHEMA IF NOT EXISTS steam;""")

if __name__ == "__main__":
    create_schema()