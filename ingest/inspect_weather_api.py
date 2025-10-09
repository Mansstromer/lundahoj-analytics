#!/usr/bin/env python3
# print_weather_info.py
"""
Fetch and print hourly weather data for Lund using the Open-Meteo API.
No database, no arguments — just prints the last 48 h and next 24 h.
"""

from __future__ import annotations
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
import requests


def http_get_with_retries(url: str, tries: int = 5, timeout: int = 30) -> Dict[str, Any]:
    delay = 1.0
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"HTTP attempt {attempt}/{tries} failed: {e}")
            if attempt == tries:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def build_url() -> str:
    lat, lon = 55.7058, 13.1932  # Lund
    past_days, forecast_days = 2, 1
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,pressure_msl,cloud_cover"
        f"&past_days={past_days}&forecast_days={forecast_days}&timezone=UTC"
    )


def fmt_ts(ts: str) -> str:
    try:
        return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return ts


def print_weather(payload: Dict[str, Any], limit: int = 24) -> None:
    hourly = payload.get("hourly", {})
    times: List[str] = hourly.get("time", []) or []
    temps = hourly.get("temperature_2m", []) or []
    precs = hourly.get("precipitation", []) or []
    winds = hourly.get("wind_speed_10m", []) or []
    hums  = hourly.get("relative_humidity_2m", []) or []
    press = hourly.get("pressure_msl", []) or []
    cloud = hourly.get("cloud_cover", []) or []

    n = len(times)
    if n == 0:
        print("No hourly data returned.")
        return

    print(f"Weather data for Lund — {n} hourly points (UTC)")
    print("-" * 80)
    rows = zip(times, temps, precs, winds, hums, press, cloud)
    for i, (t, temp, pr, w, h, p, c) in enumerate(rows):
        print(
            f"{fmt_ts(t)} | temp {temp:6.2f}°C | precip {pr:5.2f} mm | wind {w:5.2f} m/s | "
            f"RH {h:3d}% | p {p:6.1f} hPa | cloud {c:3d}%"
        )
        if i + 1 >= limit:
            break


def main() -> None:
    url = build_url()
    payload = http_get_with_retries(url)
    print_weather(payload, limit=24)  # show first 24 hours


if __name__ == "__main__":
    main()