# Lundahoj Analytics

Lundahoj Analytics is a small ELT stack for analyzing public bike share utilization in Lund, Sweden. Python ingestors land raw CityBikes and Open-Meteo snapshots in Postgres, and a dbt project models the data into hour-level aggregates that can be used for dashboards or downstream analytics.【F:ingest/ingest_bike.py†L1-L136】【F:ingest/ingest_weather.py†L1-L149】【F:lundahoj/dbt_project.yml†L5-L49】

## Architecture at a glance
```
CityBikes API ─┐            ┌─> dbt marts (future)
               ├─> raw schema in Postgres ─┬─> Staging views
Open-Meteo API ┘                           └─> Intermediate hour-level models
```
* **Ingestion:** Two idempotent Python scripts fetch station snapshots and hourly weather, then upsert into `raw.station_snapshot` and `raw.weather_hourly` tables (created on demand).【F:ingest/ingest_bike.py†L16-L129】【F:ingest/ingest_weather.py†L27-L142】
* **Transformations:** The `lundahoj` dbt project builds staging views, intermediate tables, and (placeholder) mart models in the `analytics` schema. Hourly station metrics are joined with weather features for contextual analysis.【F:lundahoj/models/2_staging/stg_stations.sql†L1-L17】【F:lundahoj/models/3_intermediate/int_station_weather_hourly.sql†L1-L57】【F:lundahoj/dbt_project.yml†L33-L49】
* **Utilities:** Lightweight CLI helpers let you inspect the source APIs without touching the warehouse, useful for troubleshooting.【F:ingest/inspect_bike_api.py†L1-L94】【F:ingest/inspect_weather_api.py†L1-L84】

## Repository layout
```
.
├── ingest/                 # Python ingestion + inspection scripts
├── lundahoj/               # dbt project (models, configs, packages)
├── logs/                   # dbt execution logs (created by dbt)
├── requirements.txt        # Python dependencies for ingestion scripts
└── README.md               # You are here
```
Key subdirectories inside `lundahoj/` follow the standard dbt convention: `models/1_sources` for source definitions, `2_staging` for renamed views, `3_intermediate` for curated tables (e.g. hourly aggregates, time spine, weather join), and `4_marts` reserved for presentation-layer models.【F:lundahoj/models/2_staging/stg_stations.sql†L1-L17】【F:lundahoj/models/3_intermediate/dim_datetime_hourly.sql†L1-L46】【F:lundahoj/models/3_intermediate/int_station_gap_fill_hourly.sql†L1-L44】

## Prerequisites
1. **Python 3.10+** with `pip` (or any modern Python able to install the dependencies in `requirements.txt`).【F:requirements.txt†L1-L3】
2. **Postgres database.** The scripts expect a Neon-hosted database, but any SSL-enabled Postgres instance works.
3. **dbt-core** and **dbt-postgres** (install via `pip install dbt-postgres` or your preferred method) to build the models.
4. Optional: cron, GitHub Actions, or another scheduler if you want to automate the ingestion scripts.

## Local environment setup
1. Clone the repository and create a virtual environment.
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   pip install dbt-postgres  # or dbt-core + adapter of your choice
   ```
2. Create a `.env` file in the project root with your Postgres connection details. The ingest scripts call `python-dotenv` and require all five variables before they run.【F:ingest/ingest_bike.py†L16-L27】
   ```dotenv
   PGHOST=...      # e.g. ep-something.aws.neon.tech
   PGPORT=5432
   PGDATABASE=...
   PGUSER=...
   PGPASSWORD=...
   ```
3. Configure dbt by adding a `profiles.yml` entry (typically in `~/.dbt/profiles.yml`). Match the credentials above and point the target schema to `analytics` (or another schema if you prefer).
   ```yaml
   lundahoj:
     outputs:
       dev:
         type: postgres
         host: ${PGHOST}
         port: 5432
         user: ${PGUSER}
         password: ${PGPASSWORD}
         dbname: ${PGDATABASE}
         schema: analytics
         threads: 4
     target: dev
   ```

## Running the ingestion scripts
Both ingestors are safe to rerun; they upsert on primary keys.

* **Bike share snapshots**
  ```bash
  python ingest/ingest_bike.py
  ```
  Creates `raw.station_snapshot` if missing, then writes one row per station per snapshot with metadata, latitude/longitude, and bike dock availability.【F:ingest/ingest_bike.py†L36-L129】

* **Hourly weather**
  ```bash
  python ingest/ingest_weather.py
  ```
  Builds/fills `raw.weather_hourly`, covering the last 48 hours and next 24 hours of hourly weather to guard against API hiccups.【F:ingest/ingest_weather.py†L40-L142】

### Inspecting APIs without loading the warehouse
For debugging upstream issues, use the lightweight inspectors:
```bash
python ingest/inspect_bike_api.py 10   # print first 10 stations
python ingest/inspect_bike_api.py raw # full JSON snapshot
python ingest/inspect_weather_api.py  # pretty-print weather timeseries
```
These scripts call the same endpoints but only display results, helping you verify API behavior, field names, and inferred capacities before ingest.【F:ingest/inspect_bike_api.py†L42-L94】【F:ingest/inspect_weather_api.py†L32-L80】

## Building the warehouse with dbt
1. Navigate into the dbt project and install packages.
   ```bash
   cd lundahoj
   dbt deps
   ```
   The project uses `dbt_utils` for data tests such as accepted ranges and uniqueness checks.【F:lundahoj/packages.yml†L1-L3】
2. Run your models.
   ```bash
   dbt build  # runs run + test for all models
   ```
   Staging models materialize as views, while intermediate and future mart layers build tables (with optional incremental configs in a `facts/` subfolder).【F:lundahoj/dbt_project.yml†L33-L49】
3. Check logs under `logs/dbt.log` if anything fails; dbt writes there by default.

## Core models
* **`stg_stations`** – Renames raw station fields, adds a `percent_full` metric, and keeps ingestion metadata for lineage.【F:lundahoj/models/2_staging/stg_stations.sql†L1-L17】
* **`stg_weather`** – Renames and exposes hourly weather metrics with explicit units.【F:lundahoj/models/2_staging/stg_weather.sql†L1-L13】
* **`dim_datetime_hourly`** – Generates a UTC hour spine plus Lund-local calendar attributes and rush-hour/weekend flags, enabling consistent joins and time intelligence.【F:lundahoj/models/3_intermediate/dim_datetime_hourly.sql†L1-L46】
* **`int_station_hourly`** – Aggregates snapshots to the hour level with averages, min/max, and sample counts per station/hour, adding indexes via `post_hook` for query performance.【F:lundahoj/models/3_intermediate/int_station_hourly.sql†L1-L35】
* **`int_station_gap_fill_hourly`** – Cross-joins stations with the time spine to expose gaps (zero samples) and provide a complete grid for heatmaps or service-level monitoring.【F:lundahoj/models/3_intermediate/int_station_gap_fill_hourly.sql†L1-L44】
* **`int_station_weather_hourly`** – Joins station metrics to aligned hourly weather observations, preserving the original weather timestamp for auditing.【F:lundahoj/models/3_intermediate/int_station_weather_hourly.sql†L1-L57】
* **`models/4_marts`** – Placeholder files for curated marts (e.g., daily station performance, network-level KPI summaries). Populate these with business-facing tables once requirements are defined.

## Testing & quality checks
`dbt build` runs data tests declared alongside the models, including non-null constraints, accepted value ranges, and unique keys. These tests catch anomalies such as negative bike counts or out-of-range humidity before they propagate to dashboards.【F:lundahoj/models/2_staging/stg_stations.yml†L1-L13】【F:lundahoj/models/2_staging/stg_weather.yml†L1-L36】【F:lundahoj/models/3_intermediate/int_station_hourly.yml†L1-L19】【F:lundahoj/models/3_intermediate/int_station_gap_fill_hourly.yml†L1-L21】

## Scheduling ideas
* Run the bike snapshot ingestor every few minutes to capture intraday utilization swings; weather can be hourly.
* Chain the dbt run after ingestion completes (e.g., cron + `dbt build`, GitHub Actions, or a managed orchestrator).
* Consider alerting on `int_station_gap_fill_hourly.has_data` to detect API outages quickly.【F:lundahoj/models/3_intermediate/int_station_gap_fill_hourly.sql†L1-L44】

## Troubleshooting tips
* Missing `PG*` variables? The ingest scripts fail fast with a descriptive error. Double-check your `.env` file and ensure it is loaded in the shell.【F:ingest/ingest_bike.py†L16-L27】
* Slow warehouse queries? Verify that dbt has run so the intermediate tables (with indexes) exist; they significantly improve hour-level filtering.【F:lundahoj/models/3_intermediate/int_station_hourly.sql†L1-L35】
* Unexpected API payload changes? Use the inspection utilities to confirm the upstream schema before updating the ingestion logic.【F:ingest/inspect_bike_api.py†L42-L94】【F:ingest/inspect_weather_api.py†L32-L80】

## Next steps
* Flesh out the mart layer with daily or network-level aggregates tailored to reporting needs.
* Add snapshots or incremental logic if historical backfills become necessary.
* Integrate orchestration (e.g., Airflow, Dagster, GitHub Actions) for automated end-to-end runs.

Happy biking! 🚲
