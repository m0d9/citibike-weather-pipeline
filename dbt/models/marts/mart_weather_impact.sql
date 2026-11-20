-- average trips per day by temperature band, on wet and dry days
select
    temp_band,
    temp_band_order,
    case when is_wet_day then 'Wet day (1 mm or more)' else 'Dry day' end  as day_type,
    count(*)                                                               as days,
    round(avg(trips))::int                                                 as avg_trips_per_day
from {{ ref('mart_daily_rides') }}
where temp_band is not null
group by temp_band, temp_band_order, is_wet_day
