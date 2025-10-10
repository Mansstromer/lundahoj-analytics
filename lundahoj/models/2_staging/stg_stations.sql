with raw as (
    select
        station_id          as station_id,
        snapshot_ts_utc     as snapshot_utc,
        free_bikes          as bikes_available,
        empty_slots         as docks_available,
        name                as station_name,
        lat                 as latitude,
        lon                 as longitude,
        source              as data_source,
        loaded_at_utc       as ingested_utc
    from {{ source('lundahoj', 'station_snapshot') }}
)
select
    *,
    (bikes_available::float / nullif((bikes_available + docks_available), 0)::float) as percent_full
from raw