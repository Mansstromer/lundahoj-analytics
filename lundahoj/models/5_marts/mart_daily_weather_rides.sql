-- Graph 4: Daily Weather vs rides (Line plot)
-- Graph 5: Weekly ride trend (Line plot)
-- Shows: daily rides, rain, wind (can be aggregated to weekly in Metabase)
-- grain: 1 row per date

{{ config(
  materialized='incremental',
  unique_key='date',
  on_schema_change="sync_all_columns") }}

with daily_rides as (
  select
    date_trunc('day', date_hour_utc)::date as date,
    sum(bikes_moved_estimate) as total_rides
  from {{ ref('int_bikes_moved_hourly') }}
  {% if is_incremental() %}
    -- only process dates we haven't seen yet (plus yesterday to be safe)
      where date_trunc('day', date_hour_utc)::date >= (
        select max(date) - interval '1 day' 
        from {{ this }}
      )
  {% endif %}
  group by 1
),
daily_weather as (
  select
    date_trunc('day', date_hour_utc)::date as date,
    sum(precip_mm) as total_rain_mm,
    avg(wind_mps) as avg_wind_mps,
    avg(temp_c) as avg_temp_c,
    avg(rel_humidity_pct) as avg_humidity_pct
  from {{ ref('stg_weather_hourly') }}
  group by 1
)
select
  r.date,
  r.total_rides as rides,
  coalesce(w.total_rain_mm, 0) as rain_mm,
  round(w.avg_wind_mps::numeric, 1) as wind_mps,
  round(w.avg_temp_c::numeric, 1) as temp_c,
  round(w.avg_humidity_pct::numeric, 1) as humidity_pct,
  extract(dow from r.date) as day_of_week,
  extract(week from r.date) as week_of_year,
  extract(year from r.date) as year
from daily_rides r
left join daily_weather w using (date)
where r.date >= now() - interval '90 days'
order by r.date
