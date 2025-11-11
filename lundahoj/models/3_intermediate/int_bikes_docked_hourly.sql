{{ config(
    materialized='incremental',
    unique_key=['station_id','date_hour_utc'],
    on_schema_change='sync_all_columns'
) }}

with snaps as (
  select
    station_id,
    date_trunc('hour', snapshot_ts_utc) as date_hour_utc,
    capacity,
    docks_available,
    snapshot_ts_utc,
    row_number() over (
      partition by station_id, date_trunc('hour', snapshot_ts_utc)
      order by snapshot_ts_utc desc
    ) as rn
  from {{ ref('stg_station_snapshot') }}
  {% if is_incremental() %}
    where snapshot_ts_utc >= (
      select coalesce(max(date_hour_utc), '1970-01-01'::timestamp) from {{ this }}
    ) - interval '48 hours'
  {% endif %}
)

select
  station_id,
  date_hour_utc,
  /* calc: bikes_docked = capacity - docks_available, clamped to [0, capacity] */
  greatest(least(capacity - docks_available, capacity), 0) as bikes_docked,
  capacity
from snaps
where rn = 1