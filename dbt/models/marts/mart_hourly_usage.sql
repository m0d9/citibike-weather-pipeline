-- average trips started in each hour, weekdays vs weekends
with per_day_hour as (
    select date_id, start_hour, count(*) as trips
    from {{ ref('fact_trips') }}
    group by date_id, start_hour
)

select
    p.start_hour                                              as hour,
    case when d.is_weekend then 'Weekend' else 'Weekday' end  as day_type,
    round(avg(p.trips))::int                                  as avg_trips
from per_day_hour p
join {{ ref('dim_date') }} d on d.date_id = p.date_id
group by p.start_hour, d.is_weekend
