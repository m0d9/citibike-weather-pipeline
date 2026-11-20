-- trips by rider type (member/casual) and bike type (classic/electric)
select
    member_casual                         as rider_type,
    rideable_type                         as bike_type,
    count(*)                              as trips,
    round(avg(duration_min)::numeric, 1)  as avg_duration_min,
    round(avg(distance_km)::numeric, 2)   as avg_distance_km
from {{ ref('fact_trips') }}
group by member_casual, rideable_type
