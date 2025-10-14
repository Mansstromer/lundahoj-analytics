-- answers: "How much does rain decrease usage?"  (analytics base table)
-- grain: station × hour
with bikes_hour as (
  select
    station_id,
    date_trunc('hour', snapshot_ts_utc) as date_hour_utc,
    avg(bikes_available::float / nullif(capacity,0)) as fill_ratio
  from {{ ref('stg_station_snapshot') }}
  group by 1,2
),
wx as (
  select
    date_hour_utc,
    precip_mm,
    temp_c,
    wind_mps,
    rel_humidity_pct
  from {{ ref('stg_weather_hourly') }}
)
select
  b.station_id,
  b.date_hour_utc,
  b.fill_ratio,
  w.precip_mm,
  w.temp_c,
  w.wind_mps,
  w.rel_humidity_pct,
  extract(dow from b.date_hour_utc)   as weekday,
  extract(hour from b.date_hour_utc)  as hour_of_day,
  case when extract(dow from b.date_hour_utc) in (0,6) then true else false end as is_weekend,
  case when extract(hour from b.date_hour_utc) between 7 and 9
     or extract(hour from b.date_hour_utc) between 16 and 18 then true else false end as is_rush_hour
from bikes_hour b
left join wx w using (date_hour_utc)
where b.fill_ratio is not null