SELECT
    station_id,
    date_hour_utc,
    GREATEST(0, prev_bikes - bikes_available) AS est_rides_started,
    GREATEST(0, bikes_available - prev_bikes) AS est_rides_ended,
    ABS(delta) / 2 AS est_total_rides
FROM {{ ref('int_station_deltas') }}