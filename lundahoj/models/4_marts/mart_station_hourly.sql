-- example mart_station_hourly.sql
select
  s.station_id,
  d.date_hour_utc,
  avg(c.bikes_available) as avg_bikes_available,
  avg(c.docks_available) as avg_docks_available,
  avg(c.bikes_available::float / nullif(c.bikes_available + c.docks_available,0)) as fill_ratio
from {{ ref('dim_datetime_hourly') }} d
cross join (select distinct station_id from {{ ref('stg_station_capacity') }}) s
left join {{ ref('stg_station_capacity') }} c
  on c.station_id = s.station_id
 and date_trunc('hour', c.snapshot_utc) = d.date_hour_utc
group by s.station_id, d.date_hour_utc
order by s.station_id, d.date_hour_utc