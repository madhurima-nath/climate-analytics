# Silver Layer: Design & Engineering Logic

This document details the engineering decisions and physical thresholds used to harmonise climate, energy, and forestry datasets.

## Pipeline Architecture

### Component Overview (Modular Architecture)

```
Modular Jobs (Run independently):

┌─────────────────────────────────────────────────────────────────┐
│  Job 1: silver_infrastructure_setup                            │
│  └─> Creates ingestion_audit table (idempotent)                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Job 2: silver_data_load                                       │
│  └─> Runs orchestrator to load/update Silver tables            │
│      (Run when you have new Bronze data)                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Job 3: silver_validation                                      │
│  └─> Runs pytest tests with smart skip logic                   │
│      ✅ Skips if no data changes since last validation          │
│      📊 Compares audit watermarks to decide                     │
└─────────────────────────────────────────────────────────────────┘


Full Pipeline (chains all three for CI/CD):

┌─────────────────────────────────────────────────────────────────┐
│            Job 4: climate_data_pipeline (Full)                │
└─────────────────────────────────────────────────────────────────┘

   Task 1                    Task 2                    Task 3
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│ setup_silver │────────>│   silver_    │────────>│  validate_   │
│     .sql     │         │ orchestrator │         │    silver    │
│              │         │      .py     │         │   _tables    │
│ Creates:     │         │              │         │              │
│ • ingestion_ │         │ Processes:   │         │ Checks:      │
│   audit tbl  │         │ • 10 configs │         │ • Watermarks │
└──────────────┘         │ • 10 tables  │         │ • 69 tests   │
                         └──────────────┘         └──────────────┘
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
       │  Transform & Standardize
       │  • Unit conversion
       │  • Thermal stress calc
       │  • H3 indexing
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

## 2. Geospatial Indexing (H3)
*   **Resolution:** Uber H3 Resolution 6 (~737 km² per cell).
*   **Logic:** Convert all coordinate-based weather data and polygon-based land use maps into a common hexagonal grid.
*   **Impact:** This enables $O(1)$ join complexity. It allows the platform to join forestry carbon flux data with historical temperature drivers without expensive "Point-in-Polygon" spatial operations.

### 2.1 Global Forest Watch Tile_ID System
Global Forest Watch (GFW) organizes their global raster datasets using a **tile-based coordinate encoding**:

*   **Format:** `{LAT}N/S_{LON}E/W` (e.g., `00N_000E`, `10N_050W`, `45S_120E`)
*   **Tile Coverage:** Each tile represents a **10° × 10°** geographic area (~1,100 km × 1,100 km at the equator)
*   **Purpose:** Efficiently manages massive global raster datasets by dividing Earth into manageable chunks

**Parsing Logic:**
```python
# Extract coordinates from tile_id
parts = tile_id.split("_")
lat = float(parts[0][:-1]) * (1 if parts[0][-1] == "N" else -1)
lon = float(parts[1][:-1]) * (1 if parts[1][-1] == "E" else -1)
```

**Implementation:** Since GFW metadata tables (peatlands, carbon flux) lack explicit latitude/longitude columns, we parse the tile_id to extract center coordinates for spatial binning and aggregation. This allows us to create spatial dimensions (`dim_h3_grid`) and perform geographic joins with weather data.

## 3. Data Persistence (Forward Fill)
*   **Logic:** Missing temperature observations are forward-filled for a maximum of 3 consecutive days.
*   **Reasoning:** Weather exhibits high persistence. Short-term sensor dropouts are best mitigated by carrying forward the last known value. Gaps exceeding 3 days remain as NULL to prevent the introduction of synthetic trends.