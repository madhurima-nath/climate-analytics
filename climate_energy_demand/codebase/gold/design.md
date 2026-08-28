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