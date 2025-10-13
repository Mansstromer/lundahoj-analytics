SELECT
    station_id,
    snapshot_utc,
    capacity_total,
    LAG(capacity_total) OVER (
        PARTITION BY station_id 
        ORDER BY snapshot_utc
    ) AS prev_capacity,
    capacity_total - LAG(capacity_total) OVER (
        PARTITION BY station_id 
        ORDER BY snapshot_utc
    ) AS delta_capacity
FROM {{ ref('stg_station_capacity') }}
WHERE capacity_total IS NOT NULL