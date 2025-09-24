import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone

DB = dict(dbname="lundahoj", user="postgres", password="4268", host="localhost", port=5432)

SQL_UPSERT_STATION = """
INSERT INTO stations (station_id, name, city, latitude, longitude, capacity, last_seen_utc)
VALUES (%s,%s,%s,%s,%s,%s,NOW())
ON CONFLICT (station_id) DO UPDATE
SET name=EXCLUDED.name,
    city=EXCLUDED.city,
    latitude=EXCLUDED.latitude,
    longitude=EXCLUDED.longitude,
    capacity=EXCLUDED.capacity,
    last_seen_utc=NOW();
"""

SQL_INSERT_READING = """
INSERT INTO station_readings
(station_id, bikes_available, docks_available, percent_full, observed_at_utc, source)
VALUES (%s,%s,%s,%s,%s,'citybikes')
ON CONFLICT (station_id, observed_at_utc) DO NOTHING;
"""

def main():
    url = "https://api.citybik.es/v2/networks/lundahoj?fields=stations,updated_at"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    net = r.json()["network"]
    stations = net.get("stations", [])
    updated_at = net.get("updated_at")
    observed_at = (
        datetime.fromisoformat(updated_at.replace("Z","+00:00")).astimezone(timezone.utc)
        if updated_at else datetime.now(timezone.utc)
    )

    station_rows, reading_rows = [], []
    for s in stations:
        sid = str(s["id"])
        name = s["name"]
        lat, lon = s["latitude"], s["longitude"]
        bikes, docks = s.get("free_bikes"), s.get("empty_slots")
        capacity = s.get("extra", {}).get("slots") or ((bikes or 0) + (docks or 0) if bikes is not None and docks is not None else None)

        station_rows.append((sid, name, None, lat, lon, capacity))
        pct = float(bikes)/(bikes+docks) if bikes is not None and docks is not None and (bikes+docks) else None
        reading_rows.append((sid, bikes, docks, pct, observed_at))

    with psycopg2.connect(**DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            if station_rows:
                execute_batch(cur, SQL_UPSERT_STATION, station_rows, page_size=100)
            if reading_rows:
                execute_batch(cur, SQL_INSERT_READING, reading_rows, page_size=500)

    print(f"Ingested {len(station_rows)} stations and {len(reading_rows)} readings at {observed_at.isoformat()}")

if __name__ == "__main__":
    main()
