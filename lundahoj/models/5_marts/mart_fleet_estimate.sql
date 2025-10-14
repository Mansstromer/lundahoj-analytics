-- models/5_marts/mart_fleet_estimate.sql
{{ config(materialized='view') }}

with system_hourly as (
  select
    date_hour_utc,
    sum(bikes_docked) as bikes_docked_total
  from {{ ref('int_bikes_docked_hourly') }}
  group by 1
),
windowed as (
  select *
  from system_hourly
  where date_hour_utc >= now() - interval '30 days'
),
coverage as (
  select
    count(*)::int as hours_present,
    (extract(epoch from (max(date_hour_utc) - min(date_hour_utc))) / 3600 + 1)::int as hours_span
  from windowed
),
ok as (
  select
    hours_present,
    hours_span,
    (hours_present::float / nullif(hours_span,0)) as coverage_pct
  from coverage
)
select
  percentile_disc(0.95) within group (order by bikes_docked_total) as fleet_estimate,
  min(date_hour_utc) as window_start_utc,
  max(date_hour_utc) as window_end_utc,
  (select coverage_pct from ok) as coverage_pct
from windowed