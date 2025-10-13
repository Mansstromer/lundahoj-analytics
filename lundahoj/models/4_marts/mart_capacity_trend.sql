SELECT
    station_id,
    DATE(snapshot_utc) AS date,
    MAX(capacity_total) AS capacity_total,
    SUM(CASE WHEN delta_capacity <> 0 THEN 1 ELSE 0 END) AS changes_count,
    MAX(delta_capacity) AS last_delta_capacity
FROM {{ ref('int_capacity_changes') }}
GROUP BY station_id, DATE(snapshot_utc)
ORDER BY date