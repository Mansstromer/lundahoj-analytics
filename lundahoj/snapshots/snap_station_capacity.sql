{% snapshot snap_station_capacity %}

{{
    config(
      target_schema='snapshots',
      unique_key='station_id',
      strategy='check',
      check_cols=['capacity', 'name', 'latitude', 'longitude'],
    )
}}

with latest_per_station as (
  select
    station_id,
    name,
    capacity,
    latitude,
    longitude,
    snapshot_ts_utc,
    row_number() over (
      partition by station_id
      order by snapshot_ts_utc desc
    ) as rn
  from {{ ref('stg_station_snapshot') }}
  where capacity is not null
)

select
    station_id,
    name,
    capacity,
    latitude,
    longitude,
    snapshot_ts_utc as updated_at
from latest_per_station
where rn = 1

{% endsnapshot %}