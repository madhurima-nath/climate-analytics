# Silver Layer: Design & Engineering Logic

This document details the engineering decisions and physical thresholds used to harmonise climate, energy, and forestry datasets.

## Pipeline Architecture

### Component Overview (Modular Architecture)

```
Modular Jobs (Run independently):

┌─────────────────────────────────────────────────────────────────┐
│  Prerequisite: project_bootstrap (one-time)                     │
│  └─> Creates catalog, schemas, audit tables, monitoring tables   │
│      (includes setup_silver.sql — creates ingestion_audit table) │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Job 1: silver_data_load                                        │
│  └─> Runs orchestrator to load/update Silver tables            │
│      (Run when you have new Bronze data)                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Job 2: silver_validation                                       │
│  └─> Runs pytest tests with smart skip logic                   │
│      ✅ Skips if no data changes since last validation          │
│      📊 Compares audit watermarks to decide                     │
└─────────────────────────────────────────────────────────────────┘


Full Pipeline (chains all layers for CI/CD):

┌─────────────────────────────────────────────────────────────────┐
│            Job 3: climate_data_pipeline (Full)                │
└─────────────────────────────────────────────────────────────────┘

   Task 1                    Task 2                    Task 3
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│ validate_    │────────>│ load_silver  │────────>│ validate_   │
│ bronze_data  │ ALL_DONE│ _data        │         │ silver_data  │
│ (non-blocking)│        │              │         │              │
└──────────────┘         └──────┬───────┘         └──────────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ load_gold_   │
                         │ data         │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │ validate_    │
                         │ gold_data    │
                         └──────────────┘

  Note: setup_silver.sql runs as part of project_bootstrap (one-time),
  not as a task in the full pipeline.
```

### File Structure

**Entry Point:** `pipelines/silver/silver_orchestrator.py` (notebook)
**Setup Script:** `pipelines/silver/setup_silver.sql`
**Configuration:** YAML files in `pipelines/silver/configs/`
**Transform Logic:** Python modules in `src/transforms/`
**Audit Utilities:** `src/common/audit_utils` (get_last_watermark, update_audit_log)

### Orchestration Flow (Per Config)

```
   ┌─────────────────────────────────────────────────────────────┐
   │  For each YAML config in pipelines/silver/configs/*.yml     │
   └────────────────────────┬────────────────────────────────────┘
                            │
                            ▼
         ┌──────────────────────────────────┐
    ┌───┤  1. Load Config & Check Watermark│
    │   └──────────────────────────────────┘
    │                   │
    │   climate_energy_demand.silver.ingestion_audit
    │   └─> last_watermark for this table
    │                   │
    │                   ▼
    │   ┌──────────────────────────────────┐
    │   │  2. EXTRACT (from Bronze)        │
    │   │                                  │
    │   │  Sources = {}                    │
    │   │  for each source in config:      │
    │   │    df = spark.table(source)      │
    │   │    filter by watermark_column    │
    │   │    sources[key] = df             │
    │   └──────────┬───────────────────────┘
    │              │
    │              ▼
    │   ┌──────────────────────────────────┐
    │   │  3. TRANSFORM                    │
    │   │                                  │
    │   │  module = src.transforms.{name}  │
    │   │  func = getattr(module, func)    │
    │   │  silver_df = func(sources, cfg)  │
    │   └──────────┬───────────────────────┘
    │              │
    │              ▼
    │   ┌──────────────────────────────────┐
    │   │  4. LOAD (MERGE or CREATE)       │
    │   │                                  │
    │   │  if not exists:                  │
    │   │    CREATE TABLE                  │
    │   │  else:                           │
    │   │    MERGE INTO target_table       │
    │   │    USING silver_df               │
    │   │    ON merge_keys                 │
    │   └──────────┬───────────────────────┘
    │              │
    │              ▼
    │   ┌──────────────────────────────────┐
    │   │  5. AUDIT                        │
    │   │                                  │
    │   │  update_audit_log(               │
    │   │    table_name,                   │
    │   │    new_watermark,                │
    │   │    rows_processed                │
    │   │  )                               │
    │   └──────────────────────────────────┘
    │
    └──> Next Config

         ┌──────────────────────────────────┐
         │  Summary Report                  │
         │  • ✅ Completed: N                │
         │  • ⏭️  Skipped: N                 │
         │  • ❌ Failed: N                   │
         └──────────────────────────────────┘
```

### Data Flow

```
┌─────────────┐
│   Bronze    │  Raw ingestion layer
│   Tables    │  • weather_raw
└──────┬──────┘  • energy_raw
       │         • forest_raw
       │
       │  Incremental Extract
       │  (watermark filtering)
       ▼
┌─────────────┐
│ src/        │  Python transformation modules
│ transforms/ │  • process_weather_historical()
└──────┬──────┘  • process_energy_metrics()
       │         • process_forest_inventory()
       │
       │  Transform & Standardise
       │  • Unit conversion
       │  • Thermal stress calc
       │  • Wide-to-long pivot
       ▼
┌─────────────┐
│   Silver    │  Cleaned & standardized layer
│   Tables    │  • weather_historical
└─────────────┘  • energy_metrics
                 • forest_inventory_annual
                 • dim_* (dimensions)

      Metadata:
┌─────────────────────────────────────────┐
│ climate_energy_demand.silver.           │
│   ingestion_audit                       │
│                                         │
│  table_name  | last_watermark | rows   │
│  weather_... | 2024-01-15     | 10000  │
│  energy_...  | 2024-01-10     | 5000   │
└─────────────────────────────────────────┘
```

## 1. Climate Stress Metrics (Degree Days)
To model the energy demand required for climate control, we implement a "Neutral Band" approach:
*   **Heating (Base 15°C):** Triggered when the daily mean temperature is below 15°C.
*   **Cooling (Base 25°C):** Triggered when the daily mean temperature exceeds 25°C.
*   **The Neutral Zone:** Temperatures between 16°C and 24°C result in 0 degree days, reflecting the "comfort zone" where buildings require minimal energy for temperature regulation.

**Reasoning:** 15°C is the standard residential heating activation threshold in the EU. 25°C represents the point where mechanical cooling (AC) demand begins to scale, specifically in temperate and tropical urban environments like Singapore.

> **Note:** These hardcoded bases (15°C/25°C) have been replaced with dynamic climate-zone-specific thresholds in the Gold layer (see Gold design doc). The Köppen-Geiger classification drives per-zone base temperatures.

## 2. Data Quality: NOAA GSOD Precipitation Sentinel

**Issue:** NOAA GSOD uses `99.99` inches as the missing-data indicator for precipitation. The silver transform (`weather.py: process_weather_observations`) was not filtering this, converting it to `2539.75mm` — a physically impossible value stored as real precipitation.

**Impact:** 14,952 rows (8.4%) across 148 of 221 stations (67%). Missing data is evenly spread across all 12 months (7.9 to 9.1% per month). 5 stations had >50% missing precipitation.

**Fix:**
- **Code:** `weather.py` now replaces `prcp = 99.99` with `NULL` before unit conversion (same pattern as temperature sentinels 9999.9/999.9).
- **One-time data cleanup:** `UPDATE silver.weather_observations SET precip_mm = NULL WHERE precip_mm = 2539.75`
- **Rationale for NULL over 0:** 0 means "no rain measured." NULL means "no measurement taken." Downstream aggregations (`SUM`/`AVG`) skip NULLs naturally.

**Downstream impact:** Gold layer Köppen-Geiger classification (`src/transforms/climate.py`) uses monthly precipitation totals. Stations with insufficient non-NULL precipitation coverage are skipped to avoid misclassification.

## 3. Data Persistence (Forward Fill)
*   **Logic:** Missing temperature observations are forward-filled for a maximum of 3 consecutive days.
*   **Reasoning:** Weather exhibits high persistence. Short-term sensor dropouts are best mitigated by carrying forward the last known value. Gaps exceeding 3 days remain as NULL to prevent the introduction of synthetic trends.