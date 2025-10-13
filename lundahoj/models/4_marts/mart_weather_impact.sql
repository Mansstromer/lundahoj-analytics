SELECT
    n.date,
    n.total_rides,
    n.mean_utilization,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_m_s,
    w.cloud_cover_pct
FROM {{ ref('mart_network_summary') }} AS n
LEFT JOIN {{ ref('stg_weather') }} AS w
  ON n.date = DATE(w.date_hour_utc)
ORDER BY n.date