{{ config(
  materialized='table',
  post_hook=[
    "create index if not exists idx_{{ this.name }}_dt on {{ this }} (date_hour_utc)",
    "create index if not exists idx_{{ this.name }}_station_dt on {{ this }} (station_id, date_hour_utc)"
  ]
) }}

with base as (
  select
    station_id,
    date_trunc('hour', snapshot_utc)                        as date_hour_utc,
    bikes_available,
    docks_available,
    percent_full
  from {{ ref('stg_stations') }}
),
agg as (
  select
    station_id,
    date_hour_utc,
    cast(avg(bikes_available) as numeric(12,1))::double precision  as bikes_available_avg,
    min(bikes_available)  as bikes_available_min,
    max(bikes_available)  as bikes_available_max,
    cast(avg(docks_available) as numeric(12,1))::double precision  as docks_available_avg,
    min(docks_available)  as docks_available_min,
    max(docks_available)  as docks_available_max,
    cast(avg(percent_full) as numeric(12,1))::double precision     as percent_full_avg,
    cast(min(percent_full) as numeric(12,1))::double precision     as percent_full_min,
    cast(max(percent_full) as numeric(12,1))::double precision     as percent_full_max,
    count(*)              as samples_per_hour
  from base
  group by 1,2
)
select * from agg