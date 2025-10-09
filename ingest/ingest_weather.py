#!/usr/bin/env python3
# ingest/weather_ingest.py
"""
Fetch Open-Meteo hourly weather for Lund and upsert into Neon Postgres.
- No args. Run locally or via scheduler.
- Creates `raw` schema and `raw.weather_hourly` if missing.
- Key = ts_utc (hour bucket, UTC). Past 48h + next 24h for robustness.
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather_ingest")

# ---------------- Env & DSN ----------------
load_dotenv()
REQUIRED = ("PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT")
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {missing}. Create a .env with PG* values from Neon Connect.")

DSN = (
    f"dbname={os.getenv('PGDATABASE')} user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT')} sslmode=require"
)

# ---------------- Constants ----------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 55.7058, 13.1932  # Lund
PAST_DAYS, FORECAST_DAYS = 2, 1

# ---------------- SQL ----------------
SQL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_hourly (
  ts_utc           timestamptz PRIMARY KEY,
  temp_c           double precision,
  precip_mm        double precision,
  wind_mps         double precision,
  rel_humidity_pct integer,
  pressure_hpa     double precision,
  cloud_cover_pct  integer,
  loaded_at_utc    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_ts ON raw.weather_hourly (ts_utc);
"""

SQL_UPSERT = """
INSERT INTO raw.weather_hourly
(ts_utc, temp_c, precip_mm, wind_mps, rel_humidity_pct, pressure_hpa, cloud_cover_pct)
VALUES %s
ON CONFLICT (ts_utc) DO UPDATE SET
  temp_c=EXCLUDED.temp_c,
  precip_mm=EXCLUDED.precip_mm,
  wind_mps=EXCLUDED.wind_mps,
  rel_humidity_pct=EXCLUDED.rel_humidity_pct,
  pressure_hpa=EXCLUDED.pressure_hpa,
  cloud_cover_pct=EXCLUDED.cloud_cover_pct;
"""

# ---------------- HTTP ----------------
def http_get_with_retries(url: str, tries: int = 5, timeout: int = 30) -> Dict[str, Any]:
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
            delay *= 2
    raise RuntimeError("unreachable")

# ---------------- Build URL ----------------
def build_url(lat: float, lon: float, past_days: int, forecast_days: int) -> str:
    params = (
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,pressure_msl,cloud_cover"
        f"&past_days={past_days}"
        f"&forecast_days={forecast_days}"
        f"&timezone=UTC"
    )
    return OPEN_METEO_URL + params

# ---------------- Parse ----------------
def parse_payload(payload: Dict[str, Any]) -> List[Tuple]:
    hourly = payload.get("hourly", {})
    times: List[str] = hourly.get("time", []) or []
    temps = hourly.get("temperature_2m", []) or []
    precs = hourly.get("precipitation", []) or []
    winds = hourly.get("wind_speed_10m", []) or []
    hums  = hourly.get("relative_humidity_2m", []) or []
    press = hourly.get("pressure_msl", []) or []
    cloud = hourly.get("cloud_cover", []) or []

    rows: List[Tuple] = []
    for t, temp, pr, w, h, p, c in zip(times, temps, precs, winds, hums, press, cloud):
        try:
            ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        rows.append((ts, float(temp), float(pr), float(w), int(h), float(p), int(c)))
    return rows

# ---------------- DB upsert ----------------
def upsert_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(SQL_CREATE)
        execute_values(cur, SQL_UPSERT, rows, page_size=1000)
    return len(rows)

# ---------------- Main ----------------
def main() -> None:
    url = build_url(LAT, LON, PAST_DAYS, FORECAST_DAYS)
    log.info("Fetching Open-Meteo hourly weather for Lund…")
    payload = http_get_with_retries(url)
    rows = parse_payload(payload)
    log.info("Rows parsed: %d", len(rows))
    n = upsert_rows(rows)
    log.info("Upserted rows: %d", n)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Weather ingest failed")
        raise