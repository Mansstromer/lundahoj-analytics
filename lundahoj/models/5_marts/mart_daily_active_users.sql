{{ config(materialized='view') }}

with system_hourly as (
  select
    date_hour_utc,
    sum(bikes_docked) as bikes_docked_total
  from {{ ref('int_bikes_docked_hourly') }}
  group by 1
),
by_day as (
  select
    date(date_hour_utc) as day,
    max(bikes_docked_total) as max_docked,
    min(bikes_docked_total) as min_docked
  from system_hourly
  group by 1
),
fleet as (
  select fleet_estimate from {{ ref('mart_fleet_estimate') }}
)
select
  d.day,
  d.max_docked - d.min_docked                           as daily_active_bikes,
  nullif((d.max_docked - d.min_docked),0)               as _nonzero_guard,
  f.fleet_estimate,
  (d.max_docked - d.min_docked)::float
    / nullif(f.fleet_estimate,0)                        as utilization_ratio
from by_day d
cross join fleet f
order by d.day desc