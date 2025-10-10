{{ config(materialized='table') }}

with bounds as (
  select
    date_trunc('hour', min(snapshot_utc))                                  as start_utc,  -- timestamptz
    date_trunc('hour', timezone('UTC', now()) + interval '1 month')        as end_utc     -- timestamptz
  from {{ ref('stg_stations') }}
),
spine as (
  -- generate timestamptz series (hourly) in UTC
  select gs::timestamptz as date_hour_utc
  from bounds b,
       generate_series(b.start_utc, b.end_utc, interval '1 hour') as gs
),
localized as (
  select
    date_hour_utc,
    -- local timestamp (no tz) in Europe/Copenhagen; DST-aware
    (date_hour_utc at time zone 'Europe/Copenhagen') as date_hour_local
  from spine
)
select
  -- canonical join key (UTC)
  date_hour_utc,

  -- UTC calendar fields
  date_trunc('day', date_hour_utc)::date                    as date_utc,
  extract(year    from date_hour_utc)::int                  as year_utc,
  extract(month   from date_hour_utc)::int                  as month_utc,
  extract(isodow  from date_hour_utc)::int                  as dow_iso_utc,
  extract(week    from date_hour_utc)::int                  as iso_week_utc,
  extract(hour    from date_hour_utc)::int                  as hour_of_day_utc,

  -- LOCAL (Europe/Copenhagen) fields — DST-aware
  date_trunc('day', date_hour_local)::date                  as date_lund,
  extract(year    from date_hour_local)::int                as year_lund,
  extract(month   from date_hour_local)::int                as month_lund,
  extract(isodow  from date_hour_local)::int                as dow_iso_lund,
  extract(week    from date_hour_local)::int                as iso_week_lund,
  extract(hour    from date_hour_local)::int                as hour_of_day_lund,
  (extract(isodow from date_hour_local) in (6,7))           as is_weekend_lund,

  -- RUSH HOUR IN LOCAL TIME (adjust ranges as needed)
  (extract(hour from date_hour_local) between 7 and 9
    or extract(hour from date_hour_local) between 15 and 18) as is_rush_hour_lund
from localized