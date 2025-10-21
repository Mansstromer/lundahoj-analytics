-- Graph 2: Hourly Occupation Trend (Bar chart)
-- Shows: average rides over hours 0-23
-- grain: 1 row per hour of day (0-23)

{{ config(materialized='table') }}

with hourly_rides as (
  select
    date_hour_utc,
    bikes_moved_estimate as rides,
    extract(hour from date_hour_utc) as hour_of_day
  from {{ ref('int_bikes_moved_hourly') }}
  where date_hour_utc >= now() - interval '30 days'
)
select
  hour_of_day,
  round(avg(rides), 1) as avg_rides,
  round(percentile_cont(0.5) within group (order by rides), 1) as median_rides,
  min(rides) as min_rides,
  max(rides) as max_rides,
  count(*) as sample_count
from hourly_rides
group by hour_of_day
order by hour_of_day
