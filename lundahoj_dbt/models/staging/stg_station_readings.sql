select
  station_id::text as station_id,
  bikes_available::int,
  docks_available::int,
  percent_full::float8,
  observed_at_utc
from public.station_readings
