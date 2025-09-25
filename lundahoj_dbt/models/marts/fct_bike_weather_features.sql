{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_fct_bw_feat_station_ts on {{ this }} (station_id, status_ts)"
    ]
) }}

with base as (
    select *
    from {{ ref('fct_bike_weather') }}
),

lagged as (
    select
        station_id,
        status_ts,
        bikes_available,
        docks_available,
        percent_full,
        avg_temp_c,
        avg_wind_m_s,
        precip_mm,

        -- Lag features (previous hour values)
        lag(percent_full, 1) over (partition by station_id order by status_ts) as percent_full_lag1,
        lag(percent_full, 2) over (partition by station_id order by status_ts) as percent_full_lag2,

        -- Rolling averages (last 3 hours including current)
        avg(percent_full) over (
            partition by station_id
            order by status_ts
            rows between 2 preceding and current row
        ) as percent_full_3h_avg,

        -- Daily aggregates (per station per day)
        sum(precip_mm) over (
            partition by station_id, date_trunc('day', status_ts)
        ) as daily_precip_mm
    from base
)

select *
from lagged
