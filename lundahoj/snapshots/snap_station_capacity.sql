{% snapshot snap_station_capacity %}

{{
    config(
      target_schema='snapshots',
      unique_key='station_id',
      strategy='check',
      check_cols=['capacity', 'name', 'latitude', 'longitude'],
    )
}}

select
    station_id,
    name,
    capacity,
    latitude,
    longitude,
    snapshot_ts_utc as updated_at
from {{ ref('stg_station_snapshot') }}
where capacity is not null

{% endsnapshot %}