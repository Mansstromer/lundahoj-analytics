SELECT
    station_id,
    DATE(date_hour_utc) AS date,
    SUM(est_rides_started) AS est_rides_started,
    SUM(est_rides_ended) AS est_rides_ended,
    SUM(est_total_rides) AS est_total_rides,
    AVG(utilization) AS avg_utilization
FROM {{ ref('mart_station_hourly') }}
GROUP BY station_id, DATE(date_hour_utc)