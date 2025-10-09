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

load_dotenv()
DSN = (
    f"dbname={os.getenv('PGDATABASE')} user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT')} sslmode=require"
)
#!/usr/bin/env python3
# ingest/bike_ingest.py
"""
Fetch Lundahoj (CityBikes) snapshot and upsert into Neon Postgres.
- No args. Run locally or via scheduler.
- Creates `raw` schema and `raw.station_snapshot` if missing.
- Key = (station_id, snapshot_ts_utc) where snapshot_ts_utc = network.updated_at (UTC).
"""



# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bike_ingest")

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
CITYBIKES_URL = "https://api.citybik.es/v2/networks/lundahoj?fields=stations,updated_at"

# ---------------- SQL ----------------
SQL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.station_snapshot (
  station_id       text NOT NULL,
  snapshot_ts_utc  timestamptz NOT NULL,
  free_bikes       integer,
  empty_slots      integer,
  capacity         integer,
  percent_full     double precision,
  name             text,
  lat              double precision,
  lon              double precision,
  source           text DEFAULT 'citybikes',
  loaded_at_utc    timestamptz DEFAULT now(),
  PRIMARY KEY (station_id, snapshot_ts_utc)
);

CREATE INDEX IF NOT EXISTS idx_station_snapshot_ts ON raw.station_snapshot (snapshot_ts_utc);
CREATE INDEX IF NOT EXISTS idx_station_snapshot_station ON raw.station_snapshot (station_id);
"""

SQL_UPSERT = """
INSERT INTO raw.station_snapshot
(station_id, snapshot_ts_utc, free_bikes, empty_slots, capacity, percent_full, name, lat, lon)
VALUES %s
ON CONFLICT (station_id, snapshot_ts_utc) DO UPDATE SET
  free_bikes   = EXCLUDED.free_bikes,
  empty_slots  = EXCLUDED.empty_slots,
  capacity     = EXCLUDED.capacity,
  percent_full = EXCLUDED.percent_full,
  name         = EXCLUDED.name,
  lat          = EXCLUDED.lat,
  lon          = EXCLUDED.lon;
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

# ---------------- Parse ----------------

def parse_payload(payload: Dict[str, Any]) -> Tuple[List[Tuple], datetime]:
    net = payload.get("network", {})
    stations = net.get("stations", []) or []

    updated_at = net.get("updated_at")
    if updated_at:
        try:
            snapshot_ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            snapshot_ts = datetime.now(timezone.utc)
    else:
        snapshot_ts = datetime.now(timezone.utc)

    rows: List[Tuple] = []
    for s in stations:
        sid = str(s.get("id"))
        name = s.get("name")
        lat = s.get("latitude")
        lon = s.get("longitude")
        bikes = s.get("free_bikes")
        docks = s.get("empty_slots")
        extra = s.get("extra") or {}

        cap = extra.get("slots")
        if cap is None and (bikes is not None and docks is not None):
            cap = (bikes or 0) + (docks or 0)

        pct = None
        if cap not in (None, 0) and bikes is not None:
            try:
                pct = float(bikes) / float(cap)
            except Exception:
                pct = None

        rows.append((sid, snapshot_ts, bikes, docks, cap, pct, name, lat, lon))

    return rows, snapshot_ts

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
    log.info("Fetching CityBikes 'lundahoj' snapshot…")
    payload = http_get_with_retries(CITYBIKES_URL)
    rows, snapshot_ts = parse_payload(payload)
    log.info("Snapshot time (UTC): %s | stations: %d", snapshot_ts.isoformat(), len(rows))
    n = upsert_rows(rows)
    log.info("Upserted rows: %d", n)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Bike ingest failed")
        raise