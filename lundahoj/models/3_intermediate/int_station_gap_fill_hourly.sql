-- models/3_intermediate/int_station_gap_fill_hourly.sql
{{ config(
  materialized='table',
  post_hook=[
    "create index if not exists idx_{{ this.name }}_dt on {{ this }} (date_hour_utc)",
    "create index if not exists idx_{{ this.name }}_station_dt on {{ this }} (station_id, date_hour_utc)"
  ]
) }}

with stations as (
  select distinct station_id
  from {{ ref('stg_stations') }}
),
spine as (
  select date_hour_utc
  from {{ ref('dim_datetime_hourly') }}
),
grid as (
  select s.station_id, t.date_hour_utc
  from stations s
  cross join spine t
),
joined as (
  select
    g.station_id,
    g.date_hour_utc,
    h.bikes_available_avg,
    h.bikes_available_min,
    h.bikes_available_max,
    h.docks_available_avg,
    h.docks_available_min,
    h.docks_available_max,
    h.percent_full_avg,
    h.percent_full_min,
    h.percent_full_max,
    coalesce(h.samples_per_hour, 0) as samples_per_hour,
    (h.station_id is not null)      as has_data
  from grid g
  left join {{ ref('int_station_hourly') }} h
    on g.station_id   = h.station_id
   and g.date_hour_utc = h.date_hour_utc
)

select * from joined