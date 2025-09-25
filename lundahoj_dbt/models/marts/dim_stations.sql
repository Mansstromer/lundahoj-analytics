{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_dim_stations_station_id on {{ this }} (station_id)"
    ]
) }}

-- one row per station_id: take latest attributes by last_seen_utc
select distinct on (station_id)
  station_id,
  station_name,
  latitude,
  longitude,
  capacity,
  active,
  first_seen_utc,
  last_seen_utc
from {{ ref('stg_stations') }}
order by station_id, last_seen_utc desc
