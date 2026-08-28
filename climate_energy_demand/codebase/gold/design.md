# Design Document: Gold Layer 

## Energy & Climate Sensitivity Story

### 1. Objective
To provide a high-performance, denormalized analytical layer that quantifies the relationship between daily climatic stress (Heating/Cooling Degree Days) and national energy consumption. This layer is optimized for **AI/BI Genie** (natural language queries) and executive-level dashboards.

### 2. Analytical Grain
*   **Primary Fact:** 1 row per `iso_code` per `date`.
*   **Reference Benchmarks:** 1 row per `iso_code` per `year`.

### 3. Data Model Architecture
*   **`gold.fct_energy_demand_daily`**: The central fact table. Combines daily weather drivers with "broadcasted" annual energy baselines.
*   **`gold.dim_locations`**: Geographic metadata (Region, Income Group, Name) used for slicing.
*   **`gold.dim_date`**: Temporal metadata (Holidays, Weekends, Quarters).
*   **`gold.ref_energy_benchmarks`**: Statistical table containing pre-calculated sensitivity coefficients (e.g., Energy-per-HDD).

### 4. Engineering & Transformation Logic
1.  **Broadcast Join:** Annual electricity consumption from `silver.energy_metrics` is joined to daily weather records on `iso_code` and `year`.
2.  **Climate Stress Calculation:** Uses daily `hdd` (Heating Degree Days) and `cdd` (Cooling Degree Days) to represent thermal demand.
3.  **Demand Sensitivity Index (DSI):** A calculated metric defined as:
    `DSI = (hdd + cdd) / (annual_electricity_consumption / 365)`
    *Purpose: Normalizes weather impact against the size of the national grid.*
4.  **Semantic Enrichment:** All columns are aliased with business-friendly names and registered with SQL `COMMENT` descriptors for AI/BI discovery.


### 5. Business Metrics (Gold Layer Specific)
*   **Heating Intensity:** Sum of `heating_degree_days` over a specific period.
*   **Grid Volatility:** Variance of the `demand_sensitivity_index` identifying countries with unhedged climate risk.
*   **Renewable Correlation:** Correlation between `avg_temperature_c` and `renewables_percentage` (useful for assessing solar/wind variability).

### 6. Performance Optimization
*   **Clustering:** `fct_energy_demand_daily` is Z-Ordered by `(iso_code, date)` to ensure sub-second response times for country-specific dashboard filters.
*   **Constraints:** Enforce `iso_code` follows ISO-3166-1 alpha-3 standards.


##  Forestry & Physical Climate Risk

### 1. Objective
To quantify the impact of physical climate stress on carbon sequestration. This layer enables high-resolution spatial analysis of how extreme heat and drought correlate with tree cover loss and carbon flux using a standardized hexagonal grid.

### 2. Analytical Grain
*   **Primary Fact:** 1 row per `h3_cell_index` (Resolution 6) per `year`.

### 3. Data Model Architecture
*   **`gold.fct_forest_resilience_annual`**: The master fact table. Merges annual forestry outcomes with aggregated climate stress metrics.
*   **`gold.dim_h3_spatial`**: Spatial dimension containing hexagon metadata (Latitude/Longitude centroids, country assignment).
*   **`gold.ref_thermal_baselines`**: Reference table storing 10-year mean temperatures per cell to calculate climate anomalies.

### 4. Engineering & Spatial Logic (The H3 Framework)
1.  **Framework:** Uses **Uber H3 Index (Resolution 6)** (~737 km² per cell).
2.  **Reasoning:** 
    *   **Computational Efficiency:** Replaces expensive $O(n^2)$ Point-in-Polygon joins with $O(1)$ string matching between weather points and forest polygons.
    *   **Area-Correctness:** Hexagons maintain equal area across latitudes, preventing the spatial distortion found in Mercator-based square grids (essential for Boreal forest analysis).
3.  **Climate Aggregation:** Daily weather records are aggregated to an annual grain to identify:
    *   `extreme_heat_days`: Annual count of days where `temp_max > 30°C`.
    *   `drought_index`: Cumulative annual days with zero precipitation.
4.  **Carbon Sequestration Efficiency (CSE):** A calculated metric: `Net_Carbon_Flux / Forest_Area`.


### 5. Business Metrics (Gold Layer Specific)
*   **Carbon-at-Risk:** Total carbon flux in cells where `extreme_heat_days` exceeds the 90th percentile.
*   **Forestry Recovery Rate:** Correlation between `ha_loss_area` and `carbon_net_flux` over a 3-year rolling window.
*   **Physical Risk Heatmap:** Visualization of `thermal_anomaly` overlaid with tree cover loss.

### 6. Performance Optimization
*   **Spatial Partitioning:** Table is partitioned by `year` and clustered by `h3_cell_index` to optimize Mapbox/Pydeck rendering in the front-end dashboard.
*   **Data Integrity:** Validates that `ha_loss_area` does not exceed the total land area of the hexagon.
