"""Load one month of clean trips and weather from the lake into the warehouse staging tables."""
import argparse
import os
from datetime import date

import psycopg2

from common import CLEAN, get_spark

TRIP_COLUMNS = [
    "ride_id", "rideable_type", "member_casual", "started_at", "ended_at", "trip_date", "start_hour",
    "duration_min", "distance_km", "start_station_id", "start_station_name", "start_lat", "start_lng",
    "end_station_id", "end_station_name", "end_lat", "end_lng",
]
WEATHER_COLUMNS = [
    "weather_date", "temp_max_c", "temp_min_c", "temp_mean_c", "precipitation_mm", "rain_mm",
    "snowfall_cm", "wind_max_kmh",
]


def month_range(month):
    start = date.fromisoformat(f"{month}-01")
    end = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
    return start, end


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ.get("WAREHOUSE_PORT", "5432")
    db = os.environ["WAREHOUSE_DB"]
    user = os.environ["WAREHOUSE_USER"]
    password = os.environ["WAREHOUSE_PASSWORD"]

    spark = get_spark("citibike-load-warehouse")
    trips = spark.read.parquet(f"{CLEAN}/trips/month={args.month}").select(*TRIP_COLUMNS)
    weather = spark.read.parquet(f"{CLEAN}/weather/month={args.month}").select(*WEATHER_COLUMNS)

    # delete the month first so a re-run does not create duplicates
    start, end = month_range(args.month)
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM staging.trips WHERE trip_date >= %s AND trip_date < %s", (start, end))
        cur.execute("DELETE FROM staging.weather WHERE weather_date >= %s AND weather_date < %s", (start, end))
    conn.close()

    url = f"jdbc:postgresql://{host}:{port}/{db}?reWriteBatchedInserts=true"
    props = {"user": user, "password": password, "driver": "org.postgresql.Driver", "batchsize": "10000"}
    trips.repartition(4).write.jdbc(url, "staging.trips", mode="append", properties=props)
    weather.write.jdbc(url, "staging.weather", mode="append", properties=props)

    print(f"loaded {trips.count()} trips and {weather.count()} weather days for {args.month}")
    spark.stop()
