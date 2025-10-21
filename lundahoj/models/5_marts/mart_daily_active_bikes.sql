{{ config(
    materialized='incremental',
    unique_key='date_day',
    on_schema_change='sync_all_columns'
) }}

with hourly as (
  select
    date_hour_utc,
    bikes_moved_estimate
  from {{ ref('int_bikes_moved_hourly') }}
  {% if is_incremental() %}
    where date_hour_utc >= (
      select coalesce(max(date_day), '1970-01-01'::date) from {{ this }}
    )::timestamp - interval '3 days'
  {% endif %}
)
select
  date_trunc('day', date_hour_utc)::date as date_day,
  sum(bikes_moved_estimate)::int       as daily_active_bikes
from hourly
group by 1
