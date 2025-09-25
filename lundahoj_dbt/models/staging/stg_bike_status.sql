with src as (
    select
        station_id::text            as station_id,
        observed_at_utc::timestamp  as status_ts,
        bikes_available::int        as bikes_available,
        docks_available::int        as docks_available,
        percent_full::double precision as percent_full
    from {{ source('public','station_readings') }}
)
select * from src
