with src as (
    select
        observed_at_utc::timestamp      as obs_ts,
        temperature_c::double precision as temp_c,
        wind_speed_mps::double precision as wind_m_s,
        precipitation_mm::double precision as precip_mm
    from {{ source('public','weather') }}
)
select * from src
