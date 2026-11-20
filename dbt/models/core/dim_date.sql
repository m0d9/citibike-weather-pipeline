-- one row per day in the loaded months, with the NYC weather for that day
with bounds as (
    select min(trip_date) as first_day, max(trip_date) as last_day
    from {{ source('staging', 'trips') }}
),

days as (
    select generate_series(first_day, last_day, interval '1 day')::date as date_id
    from bounds
)

select
    d.date_id,
    extract(year from d.date_id)::int         as year,
    extract(month from d.date_id)::int        as month,
    to_char(d.date_id, 'Mon YYYY')            as month_name,
    extract(isodow from d.date_id)::int       as day_of_week,
    to_char(d.date_id, 'Dy')                  as day_name,
    extract(isodow from d.date_id) in (6, 7)  as is_weekend,
    w.temp_mean_c,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.rain_mm,
    w.snowfall_cm,
    w.wind_max_kmh,
    coalesce(w.precipitation_mm, 0) >= 1      as is_wet_day,
    case
        when w.temp_mean_c < 0 then 'Below 0'
        when w.temp_mean_c < 10 then '0-10'
        when w.temp_mean_c < 20 then '10-20'
        when w.temp_mean_c >= 20 then '20+'
    end                                       as temp_band,  -- mean temperature in Celsius
    case
        when w.temp_mean_c < 0 then 1
        when w.temp_mean_c < 10 then 2
        when w.temp_mean_c < 20 then 3
        when w.temp_mean_c >= 20 then 4
    end                                       as temp_band_order
from days d
left join {{ source('staging', 'weather') }} w on w.weather_date = d.date_id
