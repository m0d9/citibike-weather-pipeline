-- one row per day: trips and weather (time series tile)
select
    d.date_id                                                          as ride_date,
    d.day_name,
    d.is_weekend,
    count(f.ride_id)                                                   as trips,
    count(f.ride_id) filter (where f.member_casual = 'member')         as member_trips,
    count(f.ride_id) filter (where f.member_casual = 'casual')         as casual_trips,
    count(f.ride_id) filter (where f.rideable_type = 'electric_bike')  as ebike_trips,
    round(avg(f.duration_min)::numeric, 1)                             as avg_duration_min,
    d.temp_mean_c,
    d.precipitation_mm,
    d.is_wet_day,
    d.temp_band,
    d.temp_band_order
from {{ ref('dim_date') }} d
left join {{ ref('fact_trips') }} f on f.date_id = d.date_id
group by
    d.date_id, d.day_name, d.is_weekend, d.temp_mean_c, d.precipitation_mm,
    d.is_wet_day, d.temp_band, d.temp_band_order
