#!/usr/bin/env python3
# print_api_info.py

from __future__ import annotations

import sys
import time
import json
from typing import Any, Dict
from datetime import datetime, timezone

import requests


CITYBIKES_URL = "https://api.citybik.es/v2/networks/lundahoj?fields=stations,updated_at"


def http_get_with_retries(url: str, tries: int = 5, timeout: int = 30) -> Dict[str, Any]:
    delay = 1.0
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == tries:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def iso_utc(ts: str | None) -> str:
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return ts


def main(limit: int | None = None, raw: bool = False) -> None:
    payload = http_get_with_retries(CITYBIKES_URL)

    if raw:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    net = payload.get("network", {})
    stations = net.get("stations", []) or []
    updated_at = iso_utc(net.get("updated_at"))

    print(f"Network: lundahoj")
    print(f"Updated at (UTC): {updated_at}")
    print(f"Stations returned: {len(stations)}")
    print("-" * 60)

    count = 0
    for s in stations:
        sid = str(s.get("id"))
        name = s.get("name")
        lat = s.get("latitude")
        lon = s.get("longitude")
        free_bikes = s.get("free_bikes")
        empty_slots = s.get("empty_slots")
        slots_from_extra = (s.get("extra") or {}).get("slots")
        capacity = slots_from_extra
        if capacity is None and (free_bikes is not None and empty_slots is not None):
            capacity = (free_bikes or 0) + (empty_slots or 0)

        print(f"station_id: {sid}")
        print(f"  name: {name}")
        print(f"  coords: ({lat}, {lon})")
        print(f"  free_bikes: {free_bikes}")
        print(f"  empty_slots: {empty_slots}")
        print(f"  capacity: {capacity}")
        print("-" * 60)

        count += 1
        if limit is not None and count >= limit:
            break


if __name__ == "__main__":
    # Minimal CLI:
    #   python print_api_info.py        -> pretty summary of all stations
    #   python print_api_info.py 10     -> first 10 stations
    #   python print_api_info.py raw    -> dump full JSON
    args = sys.argv[1:]
    if args and args[0] == "raw":
        main(limit=None, raw=True)
    else:
        lim = int(args[0]) if args and args[0].isdigit() else None
        main(limit=lim, raw=False)