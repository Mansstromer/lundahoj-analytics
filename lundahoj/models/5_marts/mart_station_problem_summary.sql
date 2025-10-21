-- Graph 3: Problem stations (Horizontal bar chart)
-- Shows: Top 10 stations with highest % downtime (empty or full)
-- grain: 1 row per station

{{ config(materialized='table') }}

with station_names as (
  select distinct
    station_id,
    name as station_name
  from {{ ref('stg_station_snapshot') }}
  where name is not null
),
capacity_ranked as (
  select
    station_id,
    capacity,
    row_number() over (partition by station_id order by snapshot_ts_utc desc) as rn
  from {{ ref('stg_station_snapshot') }}
  where capacity is not null
),
capacity as (
  select
    station_id,
    capacity
  from capacity_ranked
  where rn = 1
),
hourly_status as (
  select
    h.station_id,
    h.date_hour_utc,
    h.bikes_docked,
    c.capacity,
    h.bikes_docked::float / nullif(c.capacity, 0) as occupancy_ratio
  from {{ ref('int_bikes_docked_hourly') }} h
  left join capacity c using (station_id)
  where h.date_hour_utc >= now() - interval '30 days'
),
problem_calcs as (
  select
    station_id,
    count(*) as total_hours,
    sum(case when occupancy_ratio = 0 then 1 else 0 end) as hours_empty,
    sum(case when occupancy_ratio = 1 then 1 else 0 end) as hours_full
  from hourly_status
  where occupancy_ratio is not null
  group by station_id
)
select
  n.station_name,
  p.station_id,
  p.total_hours,
  p.hours_empty,
  p.hours_full,
  (p.hours_empty + p.hours_full) as hours_problem,
  round(((p.hours_empty + p.hours_full)::float / nullif(p.total_hours, 0) * 100)::numeric, 1) as downtime_pct,
  round((p.hours_empty::float / nullif(p.total_hours, 0) * 100)::numeric, 1) as empty_pct,
  round((p.hours_full::float / nullif(p.total_hours, 0) * 100)::numeric, 1) as full_pct
from problem_calcs p
left join station_names n using (station_id)
where (p.hours_empty + p.hours_full) > 0
order by downtime_pct desc
