{{ config(
    materialized='incremental',
    unique_key='date_hour_utc',
    on_schema_change='sync_all_columns'
) }}

with hourly as (
  select
    station_id,
    date_hour_utc,
    bikes_docked
  from {{ ref('int_bikes_docked_hourly') }}
  {% if is_incremental() %}
    where date_hour_utc >= (
      select coalesce(max(date_hour_utc), '1970-01-01'::timestamp) from {{ this }}
    ) - interval '72 hours'
  {% endif %}
),
deltas as (
  select
    station_id,
    date_hour_utc,
    bikes_docked
      - lag(bikes_docked) over (partition by station_id order by date_hour_utc) as delta
  from hourly
),
net_by_hour as (
  select
    date_hour_utc,
    -- arrivals are positive deltas; departures are negative
    sum(case when delta > 0 then delta else 0 end)::int as arrivals,
    sum(case when delta < 0 then -delta else 0 end)::int as departures
  from deltas
  where delta is not null
  group by 1
)
select
  date_hour_utc,
  -- use the larger of arrivals/departures to dampen imbalance noise
  greatest(arrivals, departures) as bikes_moved_estimate
from net_by_hour
