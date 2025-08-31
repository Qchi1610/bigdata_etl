--yellow taxi table
CREATE TABLE yellow_taxi_trips (
  trip_id                BIGSERIAL PRIMARY KEY,

  -- 1=Creative Mobile Technologies, 2=Curb Mobility, 6=Myle Technologies, 7=Helix
  vendorid               SMALLINT NOT NULL CHECK (vendorid IN (1,2,6,7)),

  tpep_pickup_datetime   TIMESTAMP NOT NULL,
  tpep_dropoff_datetime  TIMESTAMP NOT NULL CHECK (tpep_dropoff_datetime >= tpep_pickup_datetime),

  passenger_count        SMALLINT CHECK (passenger_count >= 0),
  trip_distance          NUMERIC(9,3) CHECK (trip_distance >= 0),

  -- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride, 99=Null/unknown
  ratecodeid             SMALLINT CHECK (ratecodeid IN (1,2,3,4,5,6,99)),

  -- Y = store and forward trip; N = not store and forward
  store_and_fwd_flag     CHAR(1) CHECK (store_and_fwd_flag IN ('Y','N')),

  pulocationid           INTEGER,  -- TLC Taxi Zone (pickup)
  dolocationid           INTEGER,  -- TLC Taxi Zone (dropoff)

  -- 0=Flex Fare, 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided trip
  payment_type           SMALLINT CHECK (payment_type IN (0,1,2,3,4,5,6)),

  fare_amount            NUMERIC(10,2) CHECK (fare_amount >= 0),
  extra                  NUMERIC(10,2) CHECK (extra >= 0),
  mta_tax                NUMERIC(10,2) CHECK (mta_tax >= 0),
  tip_amount             NUMERIC(10,2) CHECK (tip_amount >= 0),
  tolls_amount           NUMERIC(10,2) CHECK (tolls_amount >= 0),
  improvement_surcharge  NUMERIC(10,2) CHECK (improvement_surcharge >= 0),

  -- Tổng thu khách phải trả (không gồm tip tiền mặt)
  total_amount           NUMERIC(10,2),

  -- NYS congestion surcharge
  congestion_surcharge   NUMERIC(10,2) CHECK (congestion_surcharge >= 0),

  -- Chỉ áp dụng pickup tại LGA/JFK
  airport_fee            NUMERIC(10,2) CHECK (airport_fee >= 0),

  -- Per-trip charge cho MTA Congestion Relief Zone (bắt đầu 2025-01-05)
  cbd_congestion_fee     NUMERIC(10,2) CHECK (cbd_congestion_fee >= 0)
);

-- green taxi table
CREATE TABLE green_taxi_trips (
  trip_id                BIGSERIAL PRIMARY KEY,

  -- LPEP providers: 1=CMT, 2=Curb, 6=Myle
  vendorid               SMALLINT NOT NULL CHECK (vendorid IN (1,2,6)),

  lpep_pickup_datetime   TIMESTAMP NOT NULL,
  lpep_dropoff_datetime  TIMESTAMP NOT NULL CHECK (lpep_dropoff_datetime >= lpep_pickup_datetime),

  store_and_fwd_flag     CHAR(1) CHECK (store_and_fwd_flag IN ('Y','N')),

  -- TLC Taxi Zones
  pulocationid           INTEGER,
  dolocationid           INTEGER,

  passenger_count        SMALLINT CHECK (passenger_count >= 0),
  trip_distance          NUMERIC(9,3) CHECK (trip_distance >= 0),

  -- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group, 99=Null/unknown
  ratecodeid             SMALLINT CHECK (ratecodeid IN (1,2,3,4,5,6,99)),

  -- 0=Flex Fare, 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
  payment_type           SMALLINT CHECK (payment_type IN (0,1,2,3,4,5,6)),

  -- 1=Street-hail, 2=Dispatch
  trip_type              SMALLINT CHECK (trip_type IN (1,2)),

  fare_amount            NUMERIC(10,2) CHECK (fare_amount >= 0),
  extra                  NUMERIC(10,2) CHECK (extra >= 0),
  mta_tax                NUMERIC(10,2) CHECK (mta_tax >= 0),
  tip_amount             NUMERIC(10,2) CHECK (tip_amount >= 0),
  tolls_amount           NUMERIC(10,2) CHECK (tolls_amount >= 0),
  improvement_surcharge  NUMERIC(10,2) CHECK (improvement_surcharge >= 0),

  -- Tổng thu khách phải trả (không gồm tip tiền mặt)
  total_amount           NUMERIC(10,2),

  -- NYS congestion surcharge
  congestion_surcharge   NUMERIC(10,2) CHECK (congestion_surcharge >= 0),

  -- Per-trip charge cho MTA Congestion Relief Zone (bắt đầu 2025-01-05)
  cbd_congestion_fee     NUMERIC(10,2) CHECK (cbd_congestion_fee >= 0)
);