{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_fct_bw_station_ts on {{ this }} (station_id, status_ts)",
      "create index if not exists idx_fct_bw_ts on {{ this }} (status_ts)"
    ]
) }}

with avail as (
    select
        station_id,
        status_ts,
        bikes_available,
        docks_available,
        percent_full
    from {{ ref('fct_station_availability') }}
),
weather as (
    select
        hour_ts,
        avg_temp_c,
        avg_wind_m_s,
        precip_mm
    from {{ ref('fct_weather_hourly') }}
)

select
    a.station_id,
    a.status_ts,
    a.bikes_available,
    a.docks_available,
    a.percent_full,
    w.avg_temp_c,
    w.avg_wind_m_s,
    w.precip_mm
from avail a
left join weather w
  on date_trunc('hour', a.status_ts) = w.hour_ts
