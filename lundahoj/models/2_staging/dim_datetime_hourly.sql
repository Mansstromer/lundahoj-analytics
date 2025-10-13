{{ config(materialized='table') }}

with bounds as (
  select
    date_trunc('hour', min(snapshot_ts_utc)::timestamptz) as start_utc,
    date_trunc('hour', now())::timestamptz                  as end_utc
  from {{ source('lundahoj','station_snapshot') }}
),
spine as (
  select generate_series(
           (select start_utc from bounds),
           (select end_utc   from bounds),
           interval '1 hour'
         )::timestamptz as date_hour_utc
)
select
  -- canonical key
  date_hour_utc,

  -- Lund local time (handles DST automatically)
  (date_hour_utc at time zone 'Europe/Stockholm')                as date_hour_local,
  (date_hour_utc at time zone 'Europe/Stockholm')::date          as date_local,
  extract(hour from (date_hour_utc at time zone 'Europe/Stockholm'))::int as hour_local,
  extract(dow  from (date_hour_utc at time zone 'Europe/Stockholm'))::int as dow_local,
  to_char((date_hour_utc at time zone 'Europe/Stockholm'), 'Dy') as dow_name_local,

  -- Weekend classifier (Postgres: 0=Sun, 6=Sat)
  case when extract(dow from (date_hour_utc at time zone 'Europe/Stockholm')) in (0,6)
       then true else false end                                   as is_weekend_local
from spine
order by date_hour_utc