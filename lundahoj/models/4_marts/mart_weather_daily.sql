SELECT
    s.date_utc,
    AVG(s.avg_fill_ratio) AS avg_fill_ratio,
    AVG(w.temperature_c)   AS temp_c,
    AVG(w.precipitation_mm) AS precip_mm
FROM {{ ref('mart_station_daily') }} s
LEFT JOIN {{ ref('stg_weather') }} w
  ON s.date_utc = DATE(w.date_hour_utc)   -- match your weather timestamp column
GROUP BY s.date_utc
ORDER BY s.date_utc