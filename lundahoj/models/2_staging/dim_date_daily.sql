{{ config(materialized='table') }}

with bounds as (
  select
    date_trunc('day', min(snapshot_ts_utc)::timestamptz) as start_utc,
    date_trunc('day', now())::timestamptz                as end_utc
  from {{ source('lundahoj','station_snapshot') }}
),
spine as (
  select generate_series(
           (select start_utc from bounds),
           (select end_utc   from bounds),
           interval '1 day'
         )::timestamptz as date_utc
)
select
  -- canonical key
  date_utc,

  -- Lund local calendar view
  (date_utc at time zone 'Europe/Stockholm')::date                 as date_local,
  extract(dow from (date_utc at time zone 'Europe/Stockholm'))::int as dow_local,
  to_char((date_utc at time zone 'Europe/Stockholm'), 'Dy')        as dow_name_local,
  case when extract(dow from (date_utc at time zone 'Europe/Stockholm')) in (0,6)
       then true else false end                                    as is_weekend_local
from spine
order by date_utc