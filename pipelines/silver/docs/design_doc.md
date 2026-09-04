# Silver Layer: Design & Engineering Logic

This document details the engineering decisions and physical thresholds used to harmonise climate, energy, and forestry datasets.

## Pipeline Architecture

**Entry Point:** `pipelines/silver/silver_orchestrator.py` (notebook)
**Setup Script:** `pipelines/silver/setup_silver.sql`
**Configuration:** YAML files in `pipelines/silver/configs/`
**Transform Logic:** Python modules in `src/transforms/`
**Audit Utilities:** `src/common/audit_utils` (get_last_watermark, update_audit_log)

**Dependencies:**
1. `initialise_silver_infrastructure` → creates audit table
2. `run_silver_orchestrator` → processes all configs
3. `validate_silver_tables` → runs unit tests

**Orchestration Flow:**
1. Load YAML config from `pipelines/silver/configs/*.yml`
2. Check last watermark from `climate_energy_demand.silver.ingestion_audit`
3. Extract: Load source tables and filter by watermark
4. Transform: Import and execute function from `src.transforms.{module}.{function}`
5. Load: MERGE INTO target table using merge_keys
6. Audit: Update watermark and row count

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