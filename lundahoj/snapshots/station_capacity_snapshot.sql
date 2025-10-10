{% snapshot station_capacity_snapshot %}

{{
  config(
    target_schema='analytics',
    unique_key='station_id',
    strategy='check',
    check_cols=['capacity_total'],
    invalidate_hard_deletes=True
  )
}}

select
  station_id,
  capacity_total
from {{ ref('stg_station_capacity') }}

{% endsnapshot %}