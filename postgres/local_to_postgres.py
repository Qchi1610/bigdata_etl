import os
from dotenv import load_dotenv
load_dotenv()
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
from postgresql_client import PostgresSQLClient
from time import sleep

table_name = "raw.yellow_trips"
data_path = "data/yellow_tripdata_2024-01.parquet"
chunk_size = 10000

def insert_data(df):
    pstg = PostgresSQLClient(
        database = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
    )
    #lấy ttin columns
    try:
        columns = pstg.get_columns(table_name)
        print(columns)
    except Exception as e:
        print(f"Error:{e}")

    pf = ParquetFile(data_path)

    # for batch in pf.iter_batches(batch_size=chunk_size):
    #     df = batch.to_pandas()

    #     # Ép datetime thành string (nếu cần)
    #     df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].astype(str)
    #     df["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"].astype(str)
         
    #     # Chèn từng dòng vào bảng
    #     for _, row in df.iterrows():
    first_n_rows = next(pf.iter_batches(batch_size = chunk_size)) 
    df = pa.Table.from_batches([first_n_rows]).to_pandas() 
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(dtype='str')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(dtype='str')

    for _, row in df.iterrows():

        # Insert data
        query = f"""
            insert into {table_name} ({",".join(columns)})
            values {tuple(row)}
        """
        print(f"Sent: {format_record(row)}")
        pstg.execute_query(query)
        print("-"*100)
        sleep(2)
        

def format_record(row):
    taxi_res = {
        'VendorID': row['VendorID'],
        'RatecodeID': row['RatecodeID'],
        'DOLocationID': row['DOLocationID'],
        'PULocationID': row['PULocationID'],
        'payment_type': row['payment_type'],
        'tpep_dropoff_datetime': str(row['tpep_dropoff_datetime']),
        'tpep_pickup_datetime': str(row['tpep_pickup_datetime']),
        'passenger_count': row['passenger_count'],
        'trip_distance': row['trip_distance'],
        'extra': row['extra'],
        'mta_tax': row['mta_tax'],
        'fare_amount': row['fare_amount'],
        'tip_amount': row['tip_amount'],
        'tolls_amount': row['tolls_amount'],
        'total_amount': row['total_amount'],
        'improvement_surcharge': row['improvement_surcharge'],
        'congestion_surcharge': row['congestion_surcharge'],
        'Airport_fee': row['Airport_fee'],
    }
    return taxi_res

if __name__ == "__main__":
    insert_data(data_path)