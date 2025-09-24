select
  station_id::text          as station_id,
  name::text                as station_name,
  latitude::float8          as latitude,
  longitude::float8         as longitude,
  capacity::int             as capacity,
  active::boolean           as active,
  first_seen_utc,
  last_seen_utc
from public.stations