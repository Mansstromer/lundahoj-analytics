-- answers: "What is the current capacity of all stations?"  (map-ready)
-- grain: 1 row per station (latest snapshot)
with ranked as (
  select
    station_id,
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
  latitude,
  longitude,
  snapshot_ts_utc as latest_utc,
  bikes_available,
  docks_available,
  capacity
from ranked
where rn = 1
  and latitude is not null
  and longitude is not null