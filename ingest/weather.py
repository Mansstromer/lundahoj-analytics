#!/usr/bin/env python3
# ingest/weather.py

import os
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather_ingest")

# ---------------- Env & DB config ----------------
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if not ENV_PATH.exists():
    raise RuntimeError(f"Missing .env at {ENV_PATH}")
load_dotenv(ENV_PATH.as_posix(), override=True)

required = ("PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT")
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required env vars {missing} in {ENV_PATH}")

DB = dict(
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    host=os.getenv("PGHOST"),
    port=int(os.getenv("PGPORT")),
)

# ---------------- SQL ----------------
SQL_CREATE = """
CREATE TABLE IF NOT EXISTS public.weather (
  observed_at_utc   TIMESTAMPTZ PRIMARY KEY,
  temperature_c     DOUBLE PRECISION,
  precipitation_mm  DOUBLE PRECISION,
  wind_speed_mps    DOUBLE PRECISION
);
"""

SQL_UPSERT = """
INSERT INTO public.weather (observed_at_utc, temperature_c, precipitation_mm, wind_speed_mps)
VALUES (%s, %s, %s, %s)
ON CONFLICT (observed_at_utc) DO UPDATE
SET temperature_c = EXCLUDED.temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_mps = EXCLUDED.wind_speed_mps;
"""

# ---------------- Fetch ----------------
def fetch_weather(lat: float = 55.7058, lon: float = 13.1932) -> List[Tuple[datetime, float, float, float]]:
    """Fetch past 48h + next 24h hourly weather for Lund from Open-Meteo (UTC)."""
    start = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    end   = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M")
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,precipitation,wind_speed_10m"
        f"&start={start}&end={end}&timezone=UTC"
    )

    # simple retry
    delay = 1.0
    for attempt in range(5):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()["hourly"]
            rows = []
            for ts, t, p, w in zip(
                data["time"], data["temperature_2m"], data["precipitation"], data["wind_speed_10m"]
            ):
                # API returns ISO8601 UTC without offset -> mark explicitly as UTC
                observed_at = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
                rows.append((observed_at, float(t), float(p), float(w)))
            return rows
        except Exception as e:
            log.warning("Weather fetch failed (attempt %d): %s", attempt + 1, e)
            if attempt == 4:
                raise
            import time as _t
            _t.sleep(delay)
            delay *= 2

# ---------------- Upsert ----------------
def upsert_weather(rows: List[Tuple[datetime, float, float, float]]) -> int:
    if not rows:
        return 0
    with psycopg2.connect(**DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SQL_CREATE)
            execute_batch(cur, SQL_UPSERT, rows, page_size=1000)
    return len(rows)

# ---------------- Main ----------------
if __name__ == "__main__":
    log.info("Starting weather ingest")
    rows = fetch_weather()
    n = upsert_weather(rows)
    log.info("Ingested %d weather records", n)
