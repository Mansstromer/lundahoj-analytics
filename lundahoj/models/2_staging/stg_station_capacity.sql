-- one row per raw snapshot; observed capacity at that snapshot
select
  station_id,
  snapshot_utc,
  (bikes_available + docks_available) as capacity_total
from {{ ref('stg_stations') }}