import requests
from datetime import datetime, timezone

url = "https://api.citybik.es/v2/networks/lundahoj?fields=stations"
r = requests.get(url, timeout=30)
r.raise_for_status()
stations = r.json()["network"]["stations"]

print(f"Found {len(stations)} stations")
for s in stations[:3]:
    sid = s["id"]
    name = s["name"]
    lat, lon = s["latitude"], s["longitude"]
    bikes, docks = s.get("free_bikes"), s.get("empty_slots")
    # station-level timestamps vary; prefer 'timestamp', else last_updated, else now
    ts = s.get("timestamp")
    if not ts and s.get("extra", {}).get("last_updated"):
        ts = datetime.fromtimestamp(s["extra"]["last_updated"], tz=timezone.utc).isoformat()
    ts = ts or datetime.now(timezone.utc).isoformat()
    print(f"{sid:>8} | {name[:30]:30} | bikes={bikes} docks={docks} @ {ts}")
