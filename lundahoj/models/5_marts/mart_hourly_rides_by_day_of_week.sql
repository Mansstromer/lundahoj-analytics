-- models/5_marts/mart_hourly_trips_by_day_of_week.sql
{{ config(materialized='table') }}

with hourly as (
  select
    date_hour_utc,
    bikes_moved_estimate,
    extract(hour from date_hour_utc) as hour_of_day,
    extract(dow from date_hour_utc) as day_of_week,
    case 
      when extract(dow from date_hour_utc) = 0 then 'Sunday'
      when extract(dow from date_hour_utc) = 1 then 'Monday'
      when extract(dow from date_hour_utc) = 2 then 'Tuesday'
      when extract(dow from date_hour_utc) = 3 then 'Wednesday'
      when extract(dow from date_hour_utc) = 4 then 'Thursday'
      when extract(dow from date_hour_utc) = 5 then 'Friday'
      when extract(dow from date_hour_utc) = 6 then 'Saturday'
    end as day_name
  from {{ ref('int_bikes_moved_hourly') }}
  where date_hour_utc >= now() - interval '30 days'
)
select
  hour_of_day,
  day_of_week,
  day_name,
  sum(bikes_moved_estimate) as total_trips,
  avg(bikes_moved_estimate) as avg_trips_per_hour,
  count(*) as sample_size
from hourly
group by 1, 2, 3
order by 2, 1