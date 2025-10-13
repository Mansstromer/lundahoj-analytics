with stations as (
  select distinct station_id from {{ ref('stg_station_capacity') }}
),
agg as (
  select
    station_id,
    date_trunc('day', snapshot_utc)::timestamptz as date_utc,
    count(*)  as samples,
    avg(bikes_available) as avg_bikes_available,
    avg(docks_available) as avg_docks_available,
    max(capacity_total)  as capacity_total,
    avg(bikes_available::float / nullif(capacity_total,0)) as avg_fill_ratio
  from {{ ref('stg_station_capacity') }}
  group by 1,2
)
select
  s.station_id,
  d.date_utc,
  d.date_local,
  d.is_weekend_local,
  coalesce(a.samples, 0)    as samples,
  a.avg_bikes_available,
  a.avg_docks_available,
  a.capacity_total,
  a.avg_fill_ratio
from {{ ref('dim_date_daily') }} d
cross join stations s
left join agg a
  on a.station_id = s.station_id
 and a.date_utc   = d.date_utc
order by s.station_id, d.date_utc