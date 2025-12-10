# Lundahøj Analytics

A DBT analytics project for the Lundahøj bike-sharing system, providing insights into ridership patterns, weather correlations, and station operations.

## Overview

This project transforms raw bike-sharing and weather data into analytics-ready marts that power operational dashboards and strategic planning. The data pipeline follows DBT best practices with a layered architecture, ensuring data quality through testing and freshness checks.

## Data Sources

### Bike-Sharing Data
- **Source**: CityBikes API
- **Database**: Neon (PostgreSQL)
- **Schema**: `raw`
- **Table**: `station_snapshot`
- **Freshness**: Warns after 30 minutes, errors after 60 minutes
- **Key Metrics**: Station locations, available bikes, empty docks, capacity

### Weather Data
- **Source**: Weather API
- **Database**: Neon (PostgreSQL)
- **Schema**: `raw`
- **Table**: `weather_hourly`
- **Freshness**: Warns after 2 hours, errors after 4 hours
- **Metrics**: Temperature, precipitation, wind speed, humidity, pressure, cloud cover

## Architecture

The project follows a layered DBT architecture:

```
1_sources/          # Raw data source definitions
2_staging/          # Cleaned and typed data (views)
3_intermediate/     # Business logic transformations (tables)
4_core/             # (Reserved for future use)
5_marts/            # Analytics-ready tables for consumption
snapshots/          # Historical tracking (SCD Type 2)
```

### Materialization Strategy

- **Staging**: Views (no storage overhead)
- **Intermediate**: Tables (faster downstream queries)
- **Marts**: Tables (with incremental for fact tables)
- **Facts**: Incremental with merge strategy

## Key Models

### Marts (Analytics Layer)

#### `mart_daily_weather_rides`
Daily aggregation correlating ride volume with weather conditions.
- **Grain**: One row per day
- **Use Case**: Identify weather impact on ridership
- **Materialization**: Incremental

#### `mart_hourly_ride_trend`
Hourly ride demand patterns across the system.
- **Grain**: One row per hour of day (0-23)
- **Use Case**: Capacity planning and peak hour identification
- **Materialization**: Table

#### `mart_station_problem_summary`
Identifies stations with operational issues (empty/full).
- **Grain**: One row per station
- **Metrics**: Hours empty, hours full, total downtime
- **Use Case**: Rebalancing priorities
- **Materialization**: Table

#### `mart_station_status_current`
Current real-time status of all stations.
- **Grain**: One row per station
- **Use Case**: Live monitoring
- **Materialization**: Table

#### `mart_hourly_rides_by_day_of_week`
Ride patterns by day of week and hour.
- **Grain**: One row per day-of-week + hour combination
- **Use Case**: Weekly scheduling patterns
- **Materialization**: Table

### Snapshots

#### `snap_station_capacity`
Tracks historical changes to station capacity.
- **Strategy**: SCD Type 2 (timestamp-based)
- **Use Case**: Audit trail for station capacity changes

## Configuration

### Project Variables

Set in `dbt_project.yml`:

```yaml
vars:
  fleet_window_days: 7          # Rolling window for fleet calculations
  fleet_quantile: 0.99          # Percentile for fleet sizing
  min_hour_coverage_pct: 0.7    # Minimum data coverage threshold
```

### Incremental Settings

Fact tables use:
- **Strategy**: `merge` (upsert on PostgreSQL)
- **Unique Key**: `["station_id", "date_hour_utc"]`
- **Schema Changes**: `append_new_columns`

## Getting Started

### Prerequisites

- DBT Core installed
- Access to Neon database
- Valid `profiles.yml` configured for the `lundahoj` profile

### Installation

```bash
# Install DBT dependencies
dbt deps

# Verify database connection
dbt debug
```

### Running the Project

```bash
# Run all models
dbt run

# Run with full refresh (rebuilds incremental models)
dbt run --full-refresh

# Run tests
dbt test

# Build snapshots
dbt snapshot

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

## Dashboard

The project powers a Metabase dashboard with four key visualizations:

**[Lundahøj Analytics Dashboard](https://metabase-production-lundahoj.up.railway.app/dashboard/2-lundahoj-analytics)**

1. **Daily Weather vs Rides**: Correlation between weather and ridership
2. **Hourly Demand**: Average rides by hour of day
3. **Problem Stations**: Stations with highest downtime
4. **Weekly Ride Trend**: System growth and seasonal patterns

**Users**: Operations team (daily monitoring), Management (strategic planning)

## Testing

The project includes:
- **Schema Tests**: Not-null constraints on key columns
- **Source Freshness**: Automated checks for data staleness
- **Custom Tests**: Located in `tests/generic/`
- **Model Tests**: Defined in `.yml` files alongside models

## Project Owner

- **Name**: Måns Stromer
- **Email**: mans.stromer@hotmail.se

## DBT Version

- **Version**: 1.0.0
- **Profile**: `lundahoj`
- **Target Schema**: `analytics`
