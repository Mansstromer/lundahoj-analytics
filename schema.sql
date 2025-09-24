CREATE TABLE IF NOT EXISTS stations (
  station_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  capacity INTEGER,
  active BOOLEAN DEFAULT TRUE,
  first_seen_utc TIMESTAMPTZ DEFAULT NOW(),
  last_seen_utc TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS station_readings (
  reading_id BIGSERIAL PRIMARY KEY,
  station_id TEXT NOT NULL REFERENCES stations(station_id) ON UPDATE CASCADE ON DELETE CASCADE,
  bikes_available INTEGER,
  docks_available INTEGER,
  percent_full DOUBLE PRECISION,
  observed_at_utc TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'citybikes'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_station_ts ON station_readings(station_id, observed_at_utc);
CREATE INDEX IF NOT EXISTS idx_readings_ts ON station_readings(observed_at_utc);
CREATE INDEX IF NOT EXISTS idx_readings_station ON station_readings(station_id);
