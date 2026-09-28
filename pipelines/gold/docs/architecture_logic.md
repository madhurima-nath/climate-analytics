# Gold Layer: Architecture & Data Flow

## System Overview

The Gold layer is the **consumption zone** of the climate analytics platform. It transforms normalised Silver data into denormalised, analysis-ready fact tables optimised for:

* **Physical Climate Risk (PCR)** analysis
* **AI/BI Genie** natural language queries
* Executive dashboards and reporting
* Data science and ML workflows

## Architecture Pattern: Config-Driven Orchestration

### Core Components

```
┌───────────────────┐
│ YAML Config Files  │  ───────────────────────────────────>
│ (configs/*.yml)    │                                       |
└───────────────────┘                                       |
                                                           v
┌──────────────────────────────────────────────────────┐
│             Gold Orchestrator (gold_orchestrator.py)       │
│                                                              │
│  1. Load config YAML                                         │
│  2. Extract from Silver sources (full reload, no watermarking)  │
│  3. Dynamic import of transformation function                │
│  4. Execute transform (sources dict -> Gold DataFrame)       │
│  5. Merge/Insert into Gold table                             │
│  6. Update audit log                                         │
└──────────────────────────────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────┐
│         Transformation Functions (src/transforms/*.py)      │
│                                                              │
│  • energy.py:   process_energy_demand()                    │
│  • nature.py:   process_forest_resilience()               │
│  • nature.py:   process_temp_change_gold()                │
│  • nature.py:   process_land_cover_gold()                 │
│  • quality.py:  process_fidelity_audit()                  │
│  • climate.py:  transform_gold_dim_stations()             │
│  • climate.py:  transform_gold_dim_locations()            │
│  • climate.py:  transform_gold_dim_date()                 │
│  • climate.py:  transform_dim_koppen_zones()              │
└──────────────────────────────────────────────────────┘
                               |
                               v
┌──────────────────────────────────────────────────────┐
│            Gold Fact Tables (Unity Catalog)                 │
│                                                              │
│  • fct_energy_demand_daily                                │
│  • fct_temp_change_annual                                │
│  • fct_land_cover_annual                                │
│  • fct_forest_resilience_annual                          │
│  • fct_ground_truth_verification_daily                   │
│  • dim_stations, dim_locations, dim_date, dim_koppen_zones │
└──────────────────────────────────────────────────────┘
```

## Data Flow Patterns

### 1. Energy Demand Daily (Daily Grain)

**Source Tables:**
* `silver.weather_historical` (daily weather data)
* `silver.energy_metrics` (annual energy baselines)
* `silver.dim_date` (calendar attributes)
* `silver.dim_locations` (country metadata)

**Transformation Logic:**
```sql
-- Broadcast Join Pattern
weather_daily (iso_code, date, temp, hdd, cdd)
  JOIN energy_annual (iso_code, year, demand_twh) 
    ON weather.iso_code = energy.iso_code 
   AND YEAR(weather.date) = energy.year
```

**Key Calculations:**
* **Demand Sensitivity Index**: `(HDD + CDD) / (annual_demand / 365)`
* **Temperature Anomaly**: `current_temp - 10yr_monthly_baseline`
* **Heatwave Detection**: `temp_anomaly > 5°C` (EU/WMO standard)

**Grain:** 1 row per `(iso_code, date)` combination

---

### 2. Forest Resilience Annual (Country-Annual Grain)

**Source Tables:**
* `silver.forest_inventory_annual` (FAO forest area + carbon stock by country)
* `silver.weather_historical` (daily weather aggregated to annual)
* `silver.dim_locations` (country name to ISO code mapping)

**Transformation Logic:**
```sql
-- Spatial Aggregation Pattern
weather_daily (country, date, temp_max_c, temp_mean_c)
  GROUP BY country, YEAR(date)
  AGGREGATE(
    COUNT(CASE WHEN temp_max_c > 30 THEN 1 END) AS extreme_heat_days_count,
    AVG(temp_mean_c) AS avg_temp_c
  )
  JOIN forest_inventory (country_name, year, forest_area_ha, forest_carbon_mt)
    ON weather_agg.country = forest.country_name
   AND weather_agg.year = forest.year
  JOIN dim_locations (country_name, iso_code)
    ON forest.country_name = locations.country_name
```

**Key Calculations:**
* **Carbon Density**: `forest_carbon_mt / forest_area_ha`
* **Extreme Heat Days**: Count of days where `temp_max_c > 30`C
* **Avg Temperature**: Annual mean temperature per country

**Grain:** 1 row per `(iso_code, year)` combination

---

### 3. Ground Truth Verification Daily (Quality Audit)

**Source Tables:**
* `silver.weather_observations` (NOAA GSOD - physical sensors)
* `silver.weather_historical` (Open-Meteo - reanalysis model)
* `silver.dim_stations` (station metadata)

**Transformation Logic:**
```sql
-- Country-Level Join Pattern (station observations vs country modeled mean)
physical_obs (station_id, country, date, observed_temp)
  JOIN modeled_data (country, date, modeled_temp)
    ON physical.country = modeled.country
   AND physical.date = modeled.date
```

**Key Calculations:**
* **Absolute Error**: `|modeled_temp - observed_temp|`
* **Bias Error**: `modeled_temp - observed_temp` (direction matters)
* **Tolerance Flag**: `absolute_error <= 2.0°C` (WMO standard)

**Grain:** 1 row per `(station_id, date)` combination

---

## Performance Optimisations

### Z-Ordering Strategy

All fact tables are Z-Ordered on their primary join keys:

```sql
-- Energy Demand
OPTIMIZE climate_energy_demand.gold.fct_energy_demand_daily
  ZORDER BY (iso_code, date);

-- Forest Resilience  
OPTIMIZE climate_energy_demand.gold.fct_forest_resilience_annual
  ZORDER BY (iso_code, year);

-- Ground Truth
OPTIMIZE climate_energy_demand.gold.fct_ground_truth_verification_daily
  ZORDER BY (station_id, date);
```

**Result:** Sub-second query performance on dashboard filters

### Broadcast Join Optimisation

Annual dimension tables (< 10MB) are broadcast to all workers:

```python
# In transformation functions
joined = weather_df.join(
    F.broadcast(energy_df),  # Small annual table
    on=["iso_code", "year"],
    how="inner"
)
```

**Result:** No shuffle required for mismatched grain joins

---

## Observability & Audit Trail

### Audit Table Schema

```sql
CREATE TABLE climate_energy_demand.gold.ingestion_audit (
    table_name STRING,
    last_watermark TIMESTAMP,
    rows_processed INT,
    processed_at TIMESTAMP
)
```

### Lineage Tracking

Every orchestrator run records:
* **What**: Target gold table name
* **When**: Processing timestamp
* **How Much**: Row count processed
* **From Where**: Implicit via config YAML source mapping

### Quality Gates

The validation job (`gold_validation`) runs after each load:
* Primary key uniqueness checks
* Null value detection in key columns
* Business logic validation (e.g., DSI >= 0)
* Referential integrity with Silver layer

---

## AI/BI Genie Integration

### Semantic Layer (Auto-Documentation)

Every YAML config includes column-level `comments`:

```yaml
columns:
  - name: "demand_sensitivity_index"
    comment: "Calculated ratio of daily thermal stress (HDD+CDD) 
              to average daily consumption. High values indicate 
              weather-dependent grid volatility."
```

The orchestrator **automatically** registers these as Unity Catalog `COMMENT` properties, enabling Genie to understand:

* "Which countries have high heating sensitivity?" → Filter by `demand_sensitivity_index`
* "Show me forests under heat stress" → Filter by `extreme_heat_days_count`
* "Where is the model least accurate?" → Filter by `absolute_error > 2.0`

---

## Deployment via DABs

The Gold layer is deployed as part of the `climate_energy_demand` bundle:

```bash
# Standalone gold jobs (requires project_bootstrap to have been run first)
databricks bundle run gold_data_load --target dev
databricks bundle run gold_validation --target dev

# Full end-to-end pipeline (Bronze → Silver → Gold)
databricks bundle run climate_data_pipeline --target dev
```

**Job Structure:**
* Modular (each layer can run independently)
* Observable (audit logs at every step)
* Testable (pytest suite validates every table)
* Repeatable (idempotent setup, incremental loads)