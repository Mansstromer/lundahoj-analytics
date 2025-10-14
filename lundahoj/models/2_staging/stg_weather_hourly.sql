-- models/2_staging/stg_weather_hourly.sql

-- Grain: one row per timestamp (hourly)
-- Purpose: clean and rename columns

select
    ts_utc::timestamp as date_hour_utc,
    temp_c::float,
    precip_mm::float,
    wind_mps::float,
    rel_humidity_pct::int,
    pressure_hpa::float,
    cloud_cover_pct::int,
    loaded_at_utc::timestamp
from {{ source("lundahoj", "weather_hourly")}}
where ts_utc is not null