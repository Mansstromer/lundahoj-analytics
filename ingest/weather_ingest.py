import requests
import psycopg2
from datetime import datetime, timezone, timedelta

DB = dict(dbname="lundahoj", user="postgres", password="4268", host="localhost", port=5432)

def fetch_weather():
    # Lund coords (adjust if needed)
    lat, lon = 55.7058, 13.1932

    # Get past 48h + next 24h (hourly)
    start = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M")
    end = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M")

    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m"
        f"&start={start}&end={end}&timezone=UTC"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()["hourly"]

    rows = []
    for ts, t, p, w in zip(
        data["time"], data["temperature_2m"], data["precipitation"], data["wind_speed_10m"]
    ):
        observed_at = datetime.fromisoformat(ts)
        rows.append((observed_at, t, p, w))
    return rows


def upsert_weather(rows):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO public.weather (observed_at_utc, temperature_c, precipitation_mm, wind_speed_mps)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (observed_at_utc) DO UPDATE
        SET temperature_c = excluded.temperature_c,
            precipitation_mm = excluded.precipitation_mm,
            wind_speed_mps = excluded.wind_speed_mps
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    rows = fetch_weather()
    upsert_weather(rows)
    print(f"Ingested {len(rows)} weather records")
