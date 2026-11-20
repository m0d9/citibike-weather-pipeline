-- one row per station that appears in the trips, as a start or an end
with points as (
    select start_station_id as station_id, start_station_name as station_name, start_lat as lat, start_lng as lng
    from {{ source('staging', 'trips') }}
    where start_station_id is not null

    union all

    select end_station_id, end_station_name, end_lat, end_lng
    from {{ source('staging', 'trips') }}
    where end_station_id is not null
)

select
    station_id,
    max(station_name) as station_name,
    round(avg(lat) filter (where lat between 40.4 and 41.1)::numeric, 5) as lat,
    round(avg(lng) filter (where lng between -74.4 and -73.5)::numeric, 5) as lng
from points
group by station_id
