import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest")

# ---------------- Config -----------------
load_dotenv()  # reads .env if present

DB = dict(
    dbname=os.getenv("PGDATABASE", "lundahoj"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
)

CITYBIKES_URL = "https://api.citybik.es/v2/networks/lundahoj?fields=stations,updated_at"

# ---------------- SQL --------------------
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS stations (
  station_id        TEXT PRIMARY KEY,
  name              TEXT NOT NULL,
  city              TEXT,
  latitude          DOUBLE PRECISION NOT NULL,
  longitude         DOUBLE PRECISION NOT NULL,
  capacity          INTEGER,
  active            BOOLEAN DEFAULT TRUE,
  first_seen_utc    TIMESTAMPTZ DEFAULT NOW(),
  last_seen_utc     TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS station_readings (
  reading_id        BIGSERIAL PRIMARY KEY,
  station_id        TEXT NOT NULL REFERENCES stations(station_id) ON UPDATE CASCADE ON DELETE CASCADE,
  bikes_available   INTEGER,
  docks_available   INTEGER,
  percent_full      DOUBLE PRECISION,
  observed_at_utc   TIMESTAMPTZ NOT NULL,
  source            TEXT DEFAULT 'citybikes'
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_station_ts ON station_readings(station_id, observed_at_utc);
CREATE INDEX IF NOT EXISTS idx_readings_ts ON station_readings(observed_at_utc);
CREATE INDEX IF NOT EXISTS idx_readings_station ON station_readings(station_id);
"""

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

# -------------- Helpers ------------------
def http_get_with_retries(url: str, tries: int = 5, timeout: int = 30) -> Dict[str, Any]:
    """GET with exponential backoff."""
    delay = 1.0
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("HTTP attempt %d/%d failed: %s", attempt, tries, e)
            if attempt == tries:
                raise
            time.sleep(delay)
            delay *= 2  # backoff
    raise RuntimeError("unreachable")

def parse_citybikes(payload: Dict[str, Any]) -> Tuple[List[tuple], List[tuple], datetime]:
    net = payload.get("network", {})
    stations = net.get("stations", []) or []

    updated_at = net.get("updated_at")
    if updated_at:
        try:
            observed_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            observed_at = datetime.now(timezone.utc)
    else:
        observed_at = datetime.now(timezone.utc)

    station_rows, reading_rows = [], []
    for s in stations:
        sid = str(s.get("id"))
        name = s.get("name")
        lat, lon = s.get("latitude"), s.get("longitude")
        free_bikes, empty_slots = s.get("free_bikes"), s.get("empty_slots")
        capacity = s.get("extra", {}).get("slots")
        if capacity is None and (free_bikes is not None and empty_slots is not None):
            capacity = (free_bikes or 0) + (empty_slots or 0)

        station_rows.append((sid, name, None, lat, lon, capacity))

        bikes = free_bikes if free_bikes is not None else None
        docks = empty_slots if empty_slots is not None else None
        denom = (bikes or 0) + (docks or 0)
        pct = (float(bikes) / denom) if denom and bikes is not None else None

        reading_rows.append((sid, bikes, docks, pct, observed_at))

    return station_rows, reading_rows, observed_at

# --------------- Main --------------------
def main() -> None:
    log.info("Starting ingest")
    payload = http_get_with_retries(CITYBIKES_URL, tries=5, timeout=30)
    station_rows, reading_rows, observed_at = parse_citybikes(payload)
    log.info("Fetched %d stations @ %s", len(station_rows), observed_at.isoformat())

    with psycopg2.connect(**DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Ensure schema exists (idempotent)
            cur.execute(SQL_CREATE)

            if station_rows:
                execute_batch(cur, SQL_UPSERT_STATION, station_rows, page_size=500)
            if reading_rows:
                execute_batch(cur, SQL_INSERT_READING, reading_rows, page_size=1000)

    log.info("Ingest complete: %d stations, %d readings", len(station_rows), len(reading_rows))

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception:
        log.exception("Ingest failed")
        sys.exit(1)
