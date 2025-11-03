-- Graph 1: Station status overview (Table)
-- Shows: station name, bikes_available, capacity, occupancy (%)
-- grain: 1 row per station, latest snapshot

{{ config(materialized='table') }}

with ranked as (
  select
    station_id,
    name,
    snapshot_ts_utc,
    bikes_available,
    docks_available,
    capacity,
    latitude,
    longitude,
    row_number() over (
      partition by station_id
      order by snapshot_ts_utc desc
    ) as rn
  from {{ ref('stg_station_snapshot') }}
)
select
  station_id,
  name as station_name,
  bikes_available,
  capacity,
  {{ calculate_percentage('bikes_available', 'capacity') }} as occupancy_pct,
  snapshot_ts_utc as last_updated_utc,
  latitude,
  longitude
from ranked
where rn = 1
  and capacity > 0
order by station_name
