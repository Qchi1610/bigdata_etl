-- SQL commands to insert test data into the Postgres database for the demo

INSERT INTO yellow_taxi_trips (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
  passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
  pulocationid, dolocationid, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee
) VALUES
-- Row 1 (CBD, sau 2025-01-05, có tip)
(2, '2025-09-03 08:15:00', '2025-09-03 08:32:00',
  1, 3.200, 1, 'N',
  140, 236, 1,
  9.70, 1.00, 0.50, 3.15, 5.76,
  0.30, 24.16, 2.75, 0.00, 1.00),

-- Row 2 (pickup sân bay, có CBD; credit → có tip)
(2, '2025-09-03 09:05:00', '2025-09-03 09:30:00',
  2, 8.500, 1, 'N',
  138,  50, 1,
  21.63, 0.00, 0.50, 6.80, 6.55,
  0.30, 40.78, 2.75, 1.25, 1.00),

-- Row 3 (trước 2025-01-05 nên cbd_congestion_fee=0; cash → tip=0)
(1, '2024-12-15 17:40:00', '2024-12-15 18:05:00',
  1, 5.000, 2, 'Y',
  125, 132, 2,
  13.75, 1.00, 0.50, 0.00, 0.00,
  0.30, 18.30, 2.75, 0.00, 0.00);