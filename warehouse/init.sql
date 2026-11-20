-- Staging tables. Spark loads the cleaned data from the lake into these,
-- one month at a time. dbt builds the core and marts schemas from them
-- and its tests check the keys (unique ride_id, valid dates, ...).

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.trips (
    ride_id             text,
    rideable_type       text,
    member_casual       text,
    started_at          timestamp,
    ended_at            timestamp,
    trip_date           date,
    start_hour          integer,
    duration_min        double precision,
    distance_km         double precision,
    start_station_id    text,
    start_station_name  text,
    start_lat           double precision,
    start_lng           double precision,
    end_station_id      text,
    end_station_name    text,
    end_lat             double precision,
    end_lng             double precision
);

-- used to delete a month before it is loaded again
CREATE INDEX IF NOT EXISTS trips_trip_date_idx ON staging.trips (trip_date);

CREATE TABLE IF NOT EXISTS staging.weather (
    weather_date        date PRIMARY KEY,
    temp_max_c          double precision,
    temp_min_c          double precision,
    temp_mean_c         double precision,
    precipitation_mm    double precision,
    rain_mm             double precision,
    snowfall_cm         double precision,
    wind_max_kmh        double precision
);
