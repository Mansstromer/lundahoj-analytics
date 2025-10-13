SELECT
    station_id,
    snapshot_ts_utc AS snapshot_utc,
    free_bikes AS bikes_available,
    empty_slots AS docks_available,
    (free_bikes + empty_slots) AS capacity_total
FROM {{ source('lundahoj', 'station_snapshot') }}
WHERE free_bikes IS NOT NULL