import os
# from dotenv import load_dotenv
# load_dotenv()
import pyarrow as pa 
from postgresql_client import PostgresSQLClient

def create_schema():
    pstg = PostgresSQLClient(
        database = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
    )

    pstg.execute_query("""CREATE SCHEMA IF NOT EXISTS raw;""")

if __name__ == "__main__":
    create_schema()