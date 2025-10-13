with raw as (
    select
        ts_utc                  as date_hour_utc,
        temp_c                  as temperature_c,
        precip_mm               as precipitation_mm,
        wind_mps                as wind_speed_m_s,
        rel_humidity_pct        as humidity_pct,
        pressure_hpa            as pressure_hpa,
        cloud_cover_pct         as cloud_cover_pct
    from {{ source('lundahoj', 'weather_hourly') }}
)

select * from raw