-- models/2_staging/stg_station_snapshot.sql

-- Grain: one row per station_id × snapshot_ts_utc
-- Purpose: clean, typecast, and rename raw snapshot data from source('lundahoj','station_snapshot')

select
  station_id::text                as station_id,
  name::text,
  snapshot_ts_utc::timestamp      as snapshot_ts_utc,
  free_bikes::int                 as bikes_available,
  empty_slots::int                as docks_available,
  case
    when free_bikes is null or empty_slots is null then null
    else (free_bikes + empty_slots)::int
  end                             as capacity,
  lat::float                      as latitude,
  lon::float                      as longitude
from {{ source('lundahoj','station_snapshot') }}
where station_id is not null