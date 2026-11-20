"""Clean one month of raw trips and weather and write them to the clean zone as Parquet."""
import argparse

from pyspark.sql import functions as F

from common import CLEAN, RAW, get_spark


def text(name):
    # trimmed string, empty values become null
    value = F.trim(F.col(name))
    return F.when(value == "", None).otherwise(value)


def in_nyc(lat, lng):
    return lat.between(40.4, 41.1) & lng.between(-74.4, -73.5)


def haversine_km(lat1, lng1, lat2, lng2):
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlng / 2) ** 2
    return 2 * 6371 * F.asin(F.sqrt(a))


def clean_trips(spark, month):
    raw = spark.read.option("header", True).csv(f"{RAW}/citibike/{month}/")

    trips = (
        raw.select(
            text("ride_id").alias("ride_id"),
            F.lower(text("rideable_type")).alias("rideable_type"),
            F.lower(text("member_casual")).alias("member_casual"),
            F.to_timestamp("started_at").alias("started_at"),
            F.to_timestamp("ended_at").alias("ended_at"),
            text("start_station_id").alias("start_station_id"),
            text("start_station_name").alias("start_station_name"),
            F.col("start_lat").cast("double").alias("start_lat"),
            F.col("start_lng").cast("double").alias("start_lng"),
            text("end_station_id").alias("end_station_id"),
            text("end_station_name").alias("end_station_name"),
            F.col("end_lat").cast("double").alias("end_lat"),
            F.col("end_lng").cast("double").alias("end_lng"),
        )
        .filter(F.col("ride_id").isNotNull() & F.col("started_at").isNotNull() & F.col("ended_at").isNotNull())
        .dropDuplicates(["ride_id"])
        .withColumn("duration_min", F.round((F.col("ended_at").cast("long") - F.col("started_at").cast("long")) / 60, 2))
        # trips under a minute are false starts, trips over a day are lost or broken bikes
        .filter(F.col("duration_min").between(1, 24 * 60))
        # keep only trips that started in this month
        .filter(F.date_format("started_at", "yyyy-MM") == month)
        .withColumn("trip_date", F.to_date("started_at"))
        .withColumn("start_hour", F.hour("started_at"))
        .withColumn(
            "distance_km",
            F.when(
                in_nyc(F.col("start_lat"), F.col("start_lng")) & in_nyc(F.col("end_lat"), F.col("end_lng")),
                F.round(haversine_km(F.col("start_lat"), F.col("start_lng"), F.col("end_lat"), F.col("end_lng")), 3),
            ),
        )
    )

    trips.write.mode("overwrite").parquet(f"{CLEAN}/trips/month={month}")

    raw_count = raw.count()
    clean_count = spark.read.parquet(f"{CLEAN}/trips/month={month}").count()
    print(f"trips {month}: {raw_count} raw rows, {clean_count} clean rows, {raw_count - clean_count} removed")


def clean_weather(spark, month):
    raw = spark.read.option("multiLine", True).json(f"{RAW}/weather/{month}.json")

    # the API returns one array per variable, so zip them into one row per day
    daily = raw.select("daily.*")
    days = daily.select(F.explode(F.arrays_zip(*daily.columns)).alias("d")).select("d.*")

    weather = days.select(
        F.to_date("time").alias("weather_date"),
        F.col("temperature_2m_max").cast("double").alias("temp_max_c"),
        F.col("temperature_2m_min").cast("double").alias("temp_min_c"),
        F.col("temperature_2m_mean").cast("double").alias("temp_mean_c"),
        F.col("precipitation_sum").cast("double").alias("precipitation_mm"),
        F.col("rain_sum").cast("double").alias("rain_mm"),
        F.col("snowfall_sum").cast("double").alias("snowfall_cm"),
        F.col("wind_speed_10m_max").cast("double").alias("wind_max_kmh"),
    )

    weather.write.mode("overwrite").parquet(f"{CLEAN}/weather/month={month}")
    print(f"weather {month}: {weather.count()} days")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    spark = get_spark("citibike-clean-data")
    clean_trips(spark, args.month)
    clean_weather(spark, args.month)
    spark.stop()
