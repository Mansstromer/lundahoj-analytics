WITH hourly AS (
    SELECT
        station_id,
        date_hour_utc,
        bikes_available_avg AS bikes_available,
        LAG(bikes_available_avg) OVER (
            PARTITION BY station_id 
            ORDER BY date_hour_utc
        ) AS prev_bikes
    FROM {{ ref('int_station_gap_fill_hourly') }}
)

SELECT
    station_id,
    date_hour_utc,
    bikes_available,
    prev_bikes,
    bikes_available - prev_bikes AS delta
FROM hourly
WHERE prev_bikes IS NOT NULL