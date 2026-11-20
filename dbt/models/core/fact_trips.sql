-- one row per trip; station and weather details live in the dimensions
{{ config(indexes=[{'columns': ['date_id']}, {'columns': ['start_station_id']}]) }}

select
    ride_id,
    trip_date as date_id,
    start_station_id,
    end_station_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
    start_hour,
    duration_min,
    distance_km
from {{ source('staging', 'trips') }}
