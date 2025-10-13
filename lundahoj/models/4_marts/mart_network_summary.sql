SELECT
    date,
    SUM(est_rides_started) AS total_rides_started,
    SUM(est_rides_ended)   AS total_rides_ended,
    SUM(est_total_rides)   AS total_rides,
    AVG(avg_utilization)   AS mean_utilization
FROM {{ ref('mart_station_daily') }}
GROUP BY date
ORDER BY mart_station_daily.date 