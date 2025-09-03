-- Create database
CREATE DATABASE IF NOT EXISTS nyctaxi;

-- =========================
-- YELLOW TAXI
-- =========================
CREATE TABLE IF NOT EXISTS nyctaxi.yellow_taxi_trips
(
    -- surrogate id
    trip_id               UInt64,

    -- 1=CMT, 2=Curb, 6=Myle, 7=Helix
    vendorid              UInt8,          -- NOT NULL trong Postgres

    tpep_pickup_datetime  DateTime64(6),  -- NOT NULL
    tpep_dropoff_datetime DateTime64(6),  -- NOT NULL (Postgres có CHECK >= pickup)

    passenger_count       UInt16,         -- >=0
    trip_distance         Decimal(9, 3),  -- >=0

    -- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group, 99=Null/unknown
    ratecodeid            UInt8,

    -- 'Y' hoặc 'N'
    store_and_fwd_flag    LowCardinality(String),

    -- TLC Taxi Zone ids
    pulocationid          Int32,
    dolocationid          Int32,

    -- 0=Flex Fare, 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
    payment_type          UInt8,

    fare_amount           Decimal(10, 2), -- >=0
    extra                 Decimal(10, 2), -- >=0
    mta_tax               Decimal(10, 2), -- >=0
    tip_amount            Decimal(10, 2), -- >=0
    tolls_amount          Decimal(10, 2), -- >=0
    improvement_surcharge Decimal(10, 2), -- >=0

    -- tổng thu (không gồm tip tiền mặt)
    total_amount          Decimal(10, 2),

    -- NYS congestion surcharge
    congestion_surcharge  Decimal(10, 2), -- >=0

    -- chỉ áp dụng tại LGA/JFK
    airport_fee           Decimal(10, 2), -- >=0

    -- MTA Congestion Relief Zone fee (2025-01-05+)
    cbd_congestion_fee    Decimal(10, 2), -- >=0

    -- CDC helpers (tùy chọn)
    __op  LowCardinality(String) DEFAULT '',
    __lsn UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(__lsn)
PARTITION BY toYYYYMM('tpep_pickup_datetime')
ORDER BY (trip_id, tpep_pickup_datetime);

-- View (tùy chọn) để lấy "current rows" nếu bạn dùng __op từ CDC (c/u/r là hiện hành, d là xóa)
-- CREATE VIEW nyctaxi.yellow_taxi_trips_current AS
-- SELECT * FROM nyctaxi.yellow_taxi_trips WHERE (__op != 'd') OR (__op = '' OR __op IS NULL);


-- =========================
-- GREEN TAXI
-- =========================
CREATE TABLE IF NOT EXISTS nyctaxi.green_taxi_trips
(
    trip_id               UInt64,

    -- 1=CMT, 2=Curb, 6=Myle
    vendorid              UInt8,

    lpep_pickup_datetime  DateTime64(6),
    lpep_dropoff_datetime DateTime64(6),

    -- 'Y' hoặc 'N'
    store_and_fwd_flag    LowCardinality(String),

    -- TLC Taxi Zones
    pulocationid          Int32,
    dolocationid          Int32,

    passenger_count       UInt16,         -- >=0
    trip_distance         Decimal(9, 3),  -- >=0

    -- 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group, 99=Null/unknown
    ratecodeid            UInt8,

    -- 0=Flex Fare, 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
    payment_type          UInt8,

    -- 1=Street-hail, 2=Dispatch
    trip_type             UInt8,

    fare_amount           Decimal(10, 2),
    extra                 Decimal(10, 2),
    mta_tax               Decimal(10, 2),
    tip_amount            Decimal(10, 2),
    tolls_amount          Decimal(10, 2),
    improvement_surcharge Decimal(10, 2),

    -- tổng thu (không gồm tip tiền mặt)
    total_amount          Decimal(10, 2),

    -- NYS congestion surcharge
    congestion_surcharge  Decimal(10, 2),

    -- MTA Congestion Relief Zone fee (2025-01-05+)
    cbd_congestion_fee    Decimal(10, 2),

    -- CDC helpers (tùy chọn)
    __op  LowCardinality(String) DEFAULT '',
    __lsn UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(__lsn)
PARTITION BY toYYYYMM(lpep_pickup_datetime)
ORDER BY (trip_id, lpep_pickup_datetime);

-- CREATE VIEW nyctaxi.green_taxi_trips_current AS
-- SELECT * FROM nyctaxi.green_taxi_trips WHERE (__op != 'd') OR (__op = '' OR __op IS NULL);
