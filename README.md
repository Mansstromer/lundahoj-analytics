# 🚴 Lundahoj Bike-Sharing Analytics Dashboard

> Real-time analytics pipeline transforming live bike-sharing data into actionable operational insights

[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=flat&logo=metabase&logoColor=white)](https://www.metabase.com/)

**Live Data Pipeline** | Continuously updated every 20 minutes from Lundahoj API

---

## 📋 Table of Contents
- [Overview](#overview)
- [Business Problem](#business-problem)
- [Solution](#solution)
- [Tech Stack](#tech-stack)
- [Dashboard Features](#dashboard-features)
- [Key Insights](#key-insights)
- [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Development Process](#development-process)
- [Lessons Learned](#lessons-learned)

---

## 🎯 Overview

An end-to-end analytics engineering project that ingests live data from Lundahoj's bike-sharing API, transforms it using dbt, and delivers operational insights through an interactive dashboard.

**System Stats:**
- 📍 **20 stations** monitored in real-time
- 🔄 **Data ingestion** every 20 minutes via automated API calls
- 📊 **~1,400 hourly observations** per station (10 days × 2-3 records/hour)
- 🌐 **Multi-source integration**: Station API + Weather API → PostgreSQL → dbt → Metabase

**Impact:** Identifies availability issues at 4 critical stations, reveals weather patterns affecting ridership, and enables predictive maintenance scheduling.

---

## 🔍 Business Problem

Bike-sharing services face a critical operational challenge: **bikes are frequently unavailable when customers need them**.

### The Cost of Poor Availability
- **Lost Revenue:** Empty stations during peak hours mean missed rides
- **Customer Frustration:** Full stations prevent returns, forcing customers to keep riding
- **Operational Inefficiency:** Without data, rebalancing decisions are reactive and costly

### Research Questions
1. Which stations have chronic availability problems?
2. When do peak demand hours occur?
3. How does weather impact ridership?
4. Is the system growing or declining?

---

## ✅ Solution

Built a **live analytics pipeline** that continuously monitors 20 stations and surfaces insights through 5 key visualizations:

1. **Real-time Station Health Monitor** - Instant visibility into current availability
2. **Hourly Demand Patterns** - Reveals rush hour peaks for proactive rebalancing
3. **Problem Station Rankings** - Identifies infrastructure gaps requiring investment
4. **Weather Correlation Analysis** - Enables demand forecasting based on conditions
5. **Growth Trend Tracking** - Monitors business trajectory week-over-week

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Sources** | Lundahoj API + Weather API | Live station status + meteorological data |
| **Ingestion** | Python Scripts (cron) | Automated data collection every 20 min |
| **Data Warehouse** | PostgreSQL | Stores 28,000+ observations and growing |
| **Transformation** | dbt 1.10 | Dimensional modeling with incremental processing |
| **Testing** | dbt tests | Data quality validation (uniqueness, not_null, ranges) |
| **Visualization** | Metabase | Self-service operational dashboard |
| **Version Control** | Git/GitHub | Code management and documentation |

### Architecture Decisions

**Why dbt?**
- Enforces analytics engineering best practices (staging → intermediate → marts)
- Incremental models process only new data, maintaining performance at scale
- Built-in testing framework catches data quality issues automatically

**Why PostgreSQL?**
- Excellent time-series data handling with efficient indexing
- Native support for window functions crucial for delta calculations
- Open-source and widely supported

**Why Metabase?**
- Open-source BI tool perfect for operational dashboards
- Low-latency queries for real-time monitoring
- Easy for non-technical users to explore data

---

## 📊 Dashboard Features

### 1. 🚦 Station Status Overview
![Station Status](screenshots/station-status.png)

**What it shows:** Current state of all 20 stations with color-coded occupancy alerts

**Visual Indicators:**
- 🔴 **Red (0% or 100%):** Station unavailable - immediate action needed
- 🟢 **Green (1-99%):** Healthy availability for pickups and returns

**Business value:** Operations team instantly identifies which stations need rebalancing trucks dispatched

---

### 2. 📈 Hourly Demand Patterns by Day of Week
![Hourly Pattern](screenshots/hourly-pattern.png)

**What it shows:** Average rides across 24 hours, split by day of week

**Observed Patterns:**
- **Weekday double-peak:** Morning (7-9 AM) and evening (4-6 PM) commuter rushes
- **Weekend single-peak:** Midday leisure riding (11 AM-2 PM)
- **Overnight lull:** Minimal activity 11 PM-5 AM (maintenance window)

**Business value:** 
- Schedule rebalancing during identified low-demand windows
- Staff accordingly for predicted high-demand periods
- Plan maintenance during overnight lull without service disruption

---

### 3. ⚠️ Problem Stations Analysis
![Problem Stations](screenshots/problem-stations.png)

**What it shows:** Ranking of stations by downtime percentage (time spent at 0% or 100% occupancy)

**Key Findings:**
- **4 of 20 stations** (20%) have experienced availability issues in 10-day observation period
- **3 chronic problem stations** are unavailable 20%+ of the time
- Remaining 16 stations maintain healthy availability

**Business value:** 
- **Short-term:** Prioritize these 4 stations for frequent rebalancing
- **Long-term:** Evaluate capacity expansion at the 3 chronic problem stations
- **ROI calculation:** Quantify lost revenue from downtime to justify infrastructure investment

---

### 4. 🌤️ Weather Impact Analysis
![Weather Correlation](screenshots/weather-rides.png)

**What it shows:** Daily ridership plotted against weather conditions (precipitation, wind speed, temperature)

**Preliminary Findings (10 days of data):**
- **Wind speed** shows promising correlation with ride volume
- **Rainy days** show reduced ridership (more data needed for statistical significance)
- **Temperature** impact unclear with current sample size

**Next Steps:** Continue data collection to reach statistical significance (30+ days recommended)

**Business value:** 
- Forecast demand based on weather predictions
- Adjust bike deployment preemptively for weather events
- Schedule outdoor maintenance during predicted low-demand weather

---

### 5. 📉 Weekly Growth Trend
![Weekly Trend](screenshots/weekly-trend.png)

**What it shows:** Total rides per week over 10-week window

**Business value:** 
- Monitor seasonal adoption trends
- Detect impact of operational changes or marketing campaigns
- Set realistic growth targets for stakeholders

---

## 🔑 Key Insights

### Finding #1: Station Availability Is Not Uniform
**Insight:** Only 20% of stations (4 out of 20) account for all availability problems, with 3 stations experiencing >20% downtime.

**Business Impact:** Rather than system-wide interventions, targeted focus on 4 stations will yield disproportionate improvement in customer experience.

**Recommendation:** 
- Immediate: Increase rebalancing frequency for these 4 stations
- Strategic: Conduct feasibility study for adding docking capacity at the 3 chronic problem stations

---

### Finding #2: Wind Speed May Be a Leading Indicator
**Insight:** Preliminary data suggests wind speed correlates with ridership more strongly than precipitation.

**Hypothesis:** Customers tolerate light rain but avoid cycling in strong winds due to safety concerns.

**Next Steps:** Continue data collection to reach statistical significance. If confirmed, integrate wind forecasts into demand prediction model.

---

### Finding #3: Clear Rush Hour Patterns Enable Predictive Rebalancing
**Insight:** Consistent 7-9 AM and 4-6 PM peaks on weekdays create predictable rebalancing windows.

**Recommendation:** Pre-position trucks at 6 AM and 3 PM to proactively rebalance before demand spikes, rather than reactively responding to empty stations.

---

## 🏗️ Data Pipeline Architecture