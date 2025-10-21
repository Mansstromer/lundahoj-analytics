-- models/5_marts/mart_station_problem_hours.sql
{{ config(materialized='table') }}

with hourly as (
  select
    h.station_id,
    h.date_hour_utc,
    h.bikes_docked,
    c.capacity,
    h.bikes_docked::float / nullif(c.capacity, 0) as occupancy_pct
  from {{ ref('int_bikes_docked_hourly') }} h
  left join {{ ref('mart_station_capacity_current') }} c using (station_id)
  where h.date_hour_utc >= now() - interval '30 days'
),
problem_calcs as (
  select
    station_id,
    count(*) as total_hours,
    sum(case when occupancy_pct = 0 then 1 else 0 end) as hours_empty,
    sum(case when occupancy_pct = 1 then 1 else 0 end) as hours_full
  from hourly
  where occupancy_pct is not null
  group by 1
)
select
  station_id,
  total_hours,
  hours_empty,
  hours_full,
  (hours_empty + hours_full) as hours_problem,
  (hours_empty + hours_full)::float / nullif(total_hours, 0) as problem_pct,
  hours_empty::float / nullif(total_hours, 0) as empty_pct,
  hours_full::float / nullif(total_hours, 0) as full_pct
from problem_calcs
order by problem_pct desc