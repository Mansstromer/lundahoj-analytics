with src as (
    select
        station_id::text            as station_id,
        name::text                  as station_name,
        latitude::double precision  as latitude,
        longitude::double precision as longitude,
        capacity::int               as capacity,
        active::boolean             as active,
        first_seen_utc::timestamp   as first_seen_utc,
        last_seen_utc::timestamp    as last_seen_utc
    from {{ source('public','stations') }}
)
select * from src
