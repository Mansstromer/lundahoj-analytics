-- mart_station_hourly.sql
SELECT
    a.station_id,
    a.date_hour_utc,
    a.est_rides_started,
    a.est_rides_ended,
    a.est_total_rides,
    c.capacity_total,
    (c.capacity_total - g.bikes_available_avg)::float / NULLIF(c.capacity_total, 0) AS utilization
FROM {{ ref('int_station_activity') }} AS a
LEFT JOIN {{ ref('int_station_gap_fill_hourly') }} AS g
  ON a.station_id = g.station_id
  AND a.date_hour_utc = g.date_hour_utc
LEFT JOIN {{ ref('stg_station_capacity') }} AS c
  ON a.station_id = c.station_id