{{ config(materialized='table') }}

with deltas as (
  select
    station_id,
    date_hour_utc,
    bikes_docked
      - lag(bikes_docked) over (partition by station_id order by date_hour_utc) as delta
  from {{ ref('int_bikes_docked_hourly') }}
),
by_hour as (
  select
    date_trunc('hour', date_hour_utc) as date_hour_utc,
    sum(case when delta > 0 then delta else 0 end) as arrivals,
    sum(case when delta < 0 then -delta else 0 end) as departures
  from deltas
  where delta is not null
  group by 1
)
select
  date_hour_utc,
  greatest(arrivals, departures) as trips_estimate
from by_hour
order by date_hour_utc
