-- one row per raw snapshot; observed capacity at that snapshot
-- stg_station_capacity.sql
select
  station_id,
  snapshot_utc,
  (bikes_available + docks_available) as capacity_total
from {{ ref('stg_stations') }}