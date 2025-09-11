import os
# from dotenv import load_dotenv
# load_dotenv()
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
from postgresql_client import PostgresqlClient

def insert_data():
    pstg = PostgresqlClient(
        db = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD")
    )

    data_path 


import os
import time
import psycopg2
import pandas as pd

# Kết nối Postgres
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="nyc_taxi",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# Hàm insert batch
def insert_batch(df):
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO yellow_trips (
                vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id,
                payment_type, fare_amount, extra, mta_tax, tip_amount,
                tolls_amount, improvement_surcharge, total_amount,
                congestion_surcharge, airport_fee
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """, (
            row['VendorID'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime'], row['passenger_count'],
            row['trip_distance'], row['RatecodeID'], row['store_and_fwd_flag'], row['PULocationID'], row['DOLocationID'],
            row['payment_type'], row['fare_amount'], row['extra'], row['mta_tax'], row['tip_amount'],
            row['tolls_amount'], row['improvement_surcharge'], row['total_amount'],
            row.get('congestion_surcharge', 0), row.get('airport_fee', 0)
        ))
    conn.commit()

# Stream dữ liệu từ folder
data_folder = "data"   # chỗ chứa CSV
for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        print(f"Streaming {file} ...")
        file_path = os.path.join(data_folder, file)

        # Đọc CSV theo chunk để tránh out-of-memory
        for chunk in pd.read_csv(file_path, chunksize=1000):
            insert_batch(chunk)
            time.sleep(0.5)  # giả lập streaming (chậm chậm thôi)

print("✅ Done streaming all CSV files")
cur.close()
conn.close()
