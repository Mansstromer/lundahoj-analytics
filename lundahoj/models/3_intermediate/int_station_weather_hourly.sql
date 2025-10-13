{{ config(
  post_hook=[
    "create index if not exists idx_{{ this.name }}_station_dt on {{ this }} (station_id, date_hour_utc)"
  ]
) }}

with s as (
  select
    station_id,
    date_hour_utc,
    bikes_available_avg,
    bikes_available_min,
    bikes_available_max,
    docks_available_avg,
    docks_available_min,
    docks_available_max,
    percent_full_avg,
    percent_full_min,
    percent_full_max,
    samples_per_hour
  from {{ ref('int_station_hourly') }}
),
w as (
  select
    date_trunc('hour', date_hour_utc) as date_hour_utc, -- derived hour key
    date_hour_utc                     as weather_date_hour_utc, -- keep true source timestamp
    temperature_c,
    precipitation_mm,
    wind_speed_m_s,
    humidity_pct,
    pressure_hpa,
    cloud_cover_pct
  from {{ ref('stg_weather') }}
)

select
  s.station_id,
  s.date_hour_utc,
  s.bikes_available_avg,
  s.bikes_available_min,
  s.bikes_available_max,
  s.docks_available_avg,
  s.docks_available_min,
  s.docks_available_max,
  s.percent_full_avg,
  s.percent_full_min,
  s.percent_full_max,
  s.samples_per_hour,
  w.weather_date_hour_utc,  -- provenance
  w.temperature_c,
  w.precipitation_mm,
  w.wind_speed_m_s,
  w.humidity_pct,
  w.pressure_hpa,
  w.cloud_cover_pct
from s
left join w using (date_hour_utc)