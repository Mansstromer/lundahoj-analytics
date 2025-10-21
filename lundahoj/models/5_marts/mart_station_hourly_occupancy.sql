-- models/5_marts/mart_station_hourly_occupancy.sql
{{ config(materialized='table') }}

with hourly as (
  select
    station_id,
    date_hour_utc,
    bikes_docked,
    extract(hour from date_hour_utc) as hour_of_day,
    extract(dow from date_hour_utc) as day_of_week
  from {{ ref('int_bikes_docked_hourly') }}
  where date_hour_utc >= now() - interval '30 days'
)
select
  hour_of_day,
  day_of_week,
  avg(bikes_docked) as avg_bikes_docked,
  percentile_cont(0.5) within group (order by bikes_docked) as median_bikes_docked
from hourly
group by 1, 2
order by 1, 2