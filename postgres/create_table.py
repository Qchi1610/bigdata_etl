import os
from dotenv import load_dotenv
load_dotenv()
import pyarrow as pa 
from postgresql_client import PostgresSQLClient

def create_table():
    pstg = PostgresSQLClient(
        database = os.getenv("POSTGRES_DB"),
        user = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
    )

    pstg.execute_query("""CREATE TABLE IF NOT EXISTS yellow_trips (
                        trip_id                bigserial PRIMARY KEY,  -- PK để Debezium track
                        vendor_id              smallint,
                        tpep_pickup_datetime   timestamptz,
                        tpep_dropoff_datetime  timestamptz,
                        passenger_count        smallint,
                        trip_distance          numeric(7,3),
                        ratecode_id            smallint,
                        store_and_fwd_flag     char(1),
                        pu_location_id         int,
                        do_location_id         int,
                        payment_type           smallint,
                        fare_amount            numeric(9,2),
                        extra                  numeric(9,2),
                        mta_tax                numeric(9,2),
                        tip_amount             numeric(9,2),
                        tolls_amount           numeric(9,2),
                        improvement_surcharge  numeric(9,2),
                        total_amount           numeric(10,2),
                        congestion_surcharge   numeric(9,2),
                        airport_fee            numeric(9,2),
                        created_at             timestamptz DEFAULT now()
                       )""")

if __name__ == "__main__":
    create_table()