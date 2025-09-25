{{ config(
    materialized='incremental',
    unique_key=['station_id','status_ts'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with src as (
    select
        station_id,
        status_ts,
        bikes_available,
        docks_available,
        percent_full
    from {{ ref('stg_bike_status') }}
    {% if is_incremental() %}
      where status_ts > (select coalesce(max(status_ts), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
)

select *
from src
