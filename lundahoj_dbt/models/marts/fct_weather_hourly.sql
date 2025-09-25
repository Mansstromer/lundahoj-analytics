{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_fct_weather_hour_ts on {{ this }} (hour_ts)"
    ]
) }}

with base as (
  select
    date_trunc('hour', obs_ts) as hour_ts,
    temp_c,
    wind_m_s,
    precip_mm
  from {{ ref('stg_weather_hourly') }}
)
select
  hour_ts,
  avg(temp_c)    as avg_temp_c,
  avg(wind_m_s)  as avg_wind_m_s,
  sum(precip_mm) as precip_mm
from base
group by hour_ts
