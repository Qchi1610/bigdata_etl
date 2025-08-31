import os
import sys
import time
import json
import math
import random
import argparse
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# -------------------------------
# Helpers & realistic generators
# -------------------------------

def d2(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def d3(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

def choice_weighted(items_with_weights):
    items, weights = zip(*items_with_weights)
    return random.choices(items, weights=weights, k=1)[0]

def pick_vendor(is_yellow: bool) -> int:
    # Yellow: {1,2,6,7}; Green: {1,2,6}
    return random.choice([1, 2, 6, 7] if is_yellow else [1, 2, 6])

def pick_ratecode() -> int:
    # Heavily skewed toward Standard rate
    return choice_weighted([(1, 80), (2, 5), (3, 3), (4, 3), (5, 3), (6, 2), (99, 4)])

def pick_payment_type() -> int:
    # 1=Credit, 2=Cash are most common; others rare
    return choice_weighted([(1, 60), (2, 35), (3, 1), (4, 1), (5, 1), (6, 2)])

def pick_store_and_fwd_flag() -> str:
    # Mostly 'N'
    return choice_weighted([('N', 97), ('Y', 3)])

def pick_trip_type_green() -> int:
    # 1=Street-hail, 2=Dispatch
    return choice_weighted([(1, 70), (2, 30)])

def sample_passenger_count() -> int:
    # 0..6 with skew to 1~2
    return choice_weighted([(0, 5), (1, 45), (2, 35), (3, 10), (4, 3), (5, 1), (6, 1)])

def sample_trip_distance() -> Decimal:
    # 0.2 to ~25 miles, skewed to shorter trips
    base = random.expovariate(1/3.5) + 0.2  # mean ~3.5 miles
    return d3(min(base, 25.0))

def sample_duration_minutes(distance_miles: float) -> int:
    # Simple model: avg ~12 mph in city -> minutes = distance/12 * 60 + noise
    mean_minutes = (distance_miles / 12.0) * 60.0
    noise = random.uniform(-5, 15)  # -5 to +15 mins
    dur = max(1, int(mean_minutes + noise))
    return min(dur, 180)  # cap to 3 hours

def load_zone_config(config_path: str):
    if not config_path or not os.path.exists(config_path):
        return {
            "zone_count": 263,
            "airport_pickup_zones": {"JFK": [], "LGA": []},
            "cbd_zone_ids": []
        }
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("zone_count", 263)
    cfg.setdefault("airport_pickup_zones", {"JFK": [], "LGA": []})
    cfg.setdefault("cbd_zone_ids", [])
    return cfg

def pick_zones(cfg):
    zone_count = int(cfg.get("zone_count", 263))
    pu = random.randint(1, zone_count)
    do = random.randint(1, zone_count)
    # 20% chance force PU!=DO if zone_count>1
    if pu == do and zone_count > 1 and random.random() < 0.2:
        do = (pu % zone_count) + 1
    return pu, do

def is_airport_pickup(pu_zone, cfg) -> bool:
    airports = cfg.get("airport_pickup_zones", {}) or {}
    jfk = set(airports.get("JFK", []))
    lga = set(airports.get("LGA", []))
    if pu_zone in jfk or pu_zone in lga:
        return True
    # If not configured, small prob of airport pickup
    if not jfk and not lga:
        return random.random() < 0.08  # 8%
    return False

def in_cbd_area(pu_zone, do_zone, cfg) -> bool:
    cbd = set(cfg.get("cbd_zone_ids", []) or [])
    if not cbd:
        # If not configured, ~40% of trips considered in CBD scope (synthetic)
        return random.random() < 0.4
    return pu_zone in cbd or do_zone in cbd

def compute_fares(is_yellow: bool,
                  distance: Decimal,
                  ratecodeid: int,
                  payment_type: int,
                  airport_pickup: bool,
                  cbd_scope: bool,
                  pickup_dt: datetime) -> dict:
    miles = float(distance)
    # Base fare & per-mile; tweak slightly by vendor/rate
    base = 2.50 if is_yellow else 2.75
    per_mile = 2.25 if is_yellow else 2.00
    fare = max(0.0, base + miles * per_mile)

    # Extras: night/rush randomization
    hour = pickup_dt.hour
    extra = 0.0
    if 20 <= hour or hour < 6:
        extra += 0.50  # night surcharge synthetic
    if 16 <= hour < 20:
        extra += 1.00  # rush-hour surcharge synthetic

    # MTA tax (synthetic constant when fare>0)
    mta_tax = 0.50 if fare > 0 else 0.0

    # Improvement surcharge (synthetic)
    improvement_surcharge = 0.30 if fare > 0 else 0.0

    # Tolls: occasional
    tolls = 0.0
    if random.random() < 0.25:
        tolls = random.choice([5.76, 6.55, 9.50, 12.00, 17.00])

    # Congestion surcharge (synthetic)
    congestion = 0.0
    if cbd_scope:
        congestion = random.choice([2.50, 2.75]) if is_yellow else random.choice([2.00, 2.50])

    # CBD congestion fee (synthetic; applies post-2025-01-05 per schema note)
    cbd_fee = 0.0
    if pickup_dt.date() >= datetime(2025, 1, 5).date() and cbd_scope:
        cbd_fee = random.choice([0.75, 1.00, 1.25])

    # Airport fee (synthetic) if pickup from airport
    airport_fee = 1.25 if airport_pickup else 0.0

    # Tips: only for credit (1), else typically 0
    tip = 0.0
    if payment_type == 1:
        tip_pct = random.choice([0.0, 0.10, 0.15, 0.18, 0.20, 0.25])
        tip = (fare + extra + mta_tax + improvement_surcharge + congestion + cbd_fee + airport_fee + tolls) * tip_pct

    # Total
    components = [fare, extra, mta_tax, tip, tolls, improvement_surcharge, congestion, airport_fee, cbd_fee]
    total = sum(components)

    # Numeric rounding
    return {
        "fare_amount": d2(fare),
        "extra": d2(extra),
        "mta_tax": d2(mta_tax),
        "tip_amount": d2(tip),
        "tolls_amount": d2(tolls),
        "improvement_surcharge": d2(improvement_surcharge),
        "congestion_surcharge": d2(congestion),
        "airport_fee": d2(airport_fee),
        "cbd_congestion_fee": d2(cbd_fee),
        "total_amount": d2(total)
    }

def generate_yellow_rows(n: int, cfg) -> list:
    rows = []
    now = datetime.now()
    for _ in range(n):
        vendorid = pick_vendor(True)
        passenger_count = sample_passenger_count()
        distance = sample_trip_distance()
        ratecodeid = pick_ratecode()
        payment_type = pick_payment_type()
        store_and_fwd_flag = pick_store_and_fwd_flag()
        pu, do = pick_zones(cfg)

        # Pickup/dropoff times
        pickup_dt = now - timedelta(seconds=random.randint(0, 60))
        duration_min = sample_duration_minutes(float(distance))
        dropoff_dt = pickup_dt + timedelta(minutes=duration_min)

        airport_pickup = is_airport_pickup(pu, cfg)
        cbd_scope = in_cbd_area(pu, do, cfg)

        fares = compute_fares(True, distance, ratecodeid, payment_type, airport_pickup, cbd_scope, pickup_dt)

        row = (
            vendorid,
            pickup_dt, dropoff_dt,
            passenger_count,
            distance,
            ratecodeid,
            store_and_fwd_flag,
            pu, do,
            payment_type,
            fares["fare_amount"],
            fares["extra"],
            fares["mta_tax"],
            fares["tip_amount"],
            fares["tolls_amount"],
            fares["improvement_surcharge"],
            fares["total_amount"],
            fares["congestion_surcharge"],
            fares["airport_fee"],
            fares["cbd_congestion_fee"]
        )
        rows.append(row)
    return rows

def generate_green_rows(n: int, cfg) -> list:
    rows = []
    now = datetime.now()
    for _ in range(n):
        vendorid = pick_vendor(False)
        store_and_fwd_flag = pick_store_and_fwd_flag()
        pu, do = pick_zones(cfg)
        passenger_count = sample_passenger_count()
        distance = sample_trip_distance()
        ratecodeid = pick_ratecode()
        payment_type = pick_payment_type()
        trip_type = pick_trip_type_green()

        pickup_dt = now - timedelta(seconds=random.randint(0, 60))
        duration_min = sample_duration_minutes(float(distance))
        dropoff_dt = pickup_dt + timedelta(minutes=duration_min)

        airport_pickup = is_airport_pickup(pu, cfg)
        cbd_scope = in_cbd_area(pu, do, cfg)

        fares = compute_fares(False, distance, ratecodeid, payment_type, airport_pickup, cbd_scope, pickup_dt)

        row = (
            vendorid,
            pickup_dt, dropoff_dt,
            store_and_fwd_flag,
            pu, do,
            passenger_count,
            distance,
            ratecodeid,
            payment_type,
            trip_type,
            fares["fare_amount"],
            fares["extra"],
            fares["mta_tax"],
            fares["tip_amount"],
            fares["tolls_amount"],
            fares["improvement_surcharge"],
            fares["total_amount"],
            fares["congestion_surcharge"],
            fares["cbd_congestion_fee"]
        )
        rows.append(row)
    return rows

# -------------------------------
# DB Insertion
# -------------------------------

YELLOW_INSERT = """
INSERT INTO public.yellow_taxi_trips (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
  passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
  pulocationid, dolocationid, payment_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee
) VALUES %s
"""

GREEN_INSERT = """
INSERT INTO public.green_taxi_trips (
  vendorid, lpep_pickup_datetime, lpep_dropoff_datetime,
  store_and_fwd_flag, pulocationid, dolocationid,
  passenger_count, trip_distance, ratecodeid, payment_type, trip_type,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, cbd_congestion_fee
) VALUES %s
"""

def connect_db(args):
    conn = psycopg2.connect(
        dbname=args.dbname or os.environ.get("PGDATABASE", "inventory"),
        user=args.user or os.environ.get("PGUSER", "postgres"),
        password=args.password or os.environ.get("PGPASSWORD", "postgres"),
        host=args.host or os.environ.get("PGHOST", "postgres"),
        port=args.port or os.environ.get("PGPORT", "5432"),
    )
    conn.autocommit = False
    return conn

# -------------------------------
# Main loop
# -------------------------------

def main():
    parser = argparse.ArgumentParser(description="NYC Taxi data generator for Yellow & Green tables")
    parser.add_argument("--yellow", type=int, default=0, help="Records per cycle for yellow_taxi_trips")
    parser.add_argument("--green", type=int, default=0, help="Records per cycle for green_taxi_trips")
    parser.add_argument("--sleep", type=float, default=5.0, help="Seconds to sleep between cycles")
    parser.add_argument("--loops", type=int, default=0, help="Number of cycles to run (0 = infinite)")
    parser.add_argument("--zone-config", type=str, default="", help="Optional JSON config for airport/CBD zone IDs")
    parser.add_argument("--host", type=str, default="", help="Postgres host (default env PGHOST or 'postgres')")
    parser.add_argument("--port", type=str, default="", help="Postgres port (default env PGPORT or '5432')")
    parser.add_argument("--user", type=str, default="", help="Postgres user (default env PGUSER or 'postgres')")
    parser.add_argument("--password", type=str, default="", help="Postgres password (default env PGPASSWORD or 'postgres')")
    parser.add_argument("--dbname", type=str, default="", help="Postgres database name (default env PGDATABASE or 'inventory')")
    args = parser.parse_args()

    if args.yellow <= 0 and args.green <= 0:
        print("Nothing to generate. Use --yellow and/or --green > 0")
        sys.exit(0)

    cfg = load_zone_config(args.zone_config)
    conn = connect_db(args)
    cur = conn.cursor()

    # small warm-up for Debezium demo
    time.sleep(2)

    loop = 0
    try:
        while True:
            loop += 1
            if args.loops and loop > args.loops:
                break

            total_inserted = 0

            if args.yellow > 0:
                y_rows = generate_yellow_rows(args.yellow, cfg)
                execute_values(cur, YELLOW_INSERT, y_rows, page_size=max(100, args.yellow))
                total_inserted += len(y_rows)

            if args.green > 0:
                g_rows = generate_green_rows(args.green, cfg)
                execute_values(cur, GREEN_INSERT, g_rows, page_size=max(100, args.green))
                total_inserted += len(g_rows)

            conn.commit()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] loop={loop} inserted={total_inserted} rows")
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    # small delay to wait for DB in containerized setups
    time.sleep(3)
    main()