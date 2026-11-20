-- trips started and ended at each station
with starts as (
    select start_station_id as station_id, count(*) as trips_started
    from {{ ref('fact_trips') }}
    group by start_station_id
),

ends as (
    select end_station_id as station_id, count(*) as trips_ended
    from {{ ref('fact_trips') }}
    group by end_station_id
)

select
    s.station_id,
    s.station_name,
    s.lat,
    s.lng,
    coalesce(st.trips_started, 0)  as trips_started,
    coalesce(en.trips_ended, 0)    as trips_ended
from {{ ref('dim_station') }} s
left join starts st on st.station_id = s.station_id
left join ends en on en.station_id = s.station_id
