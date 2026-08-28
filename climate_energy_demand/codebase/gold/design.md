# Design Document: Gold Layer Intelligence Platform

## 1. Global Engineering Standards
To ensure a senior-level, production-grade platform, all Gold-layer tables adhere to these architectural standards:

*   **Config-Driven Orchestration:** All table schemas, source mappings, and metadata are defined in YAML. Transformations are implemented in isolated Python functions.
*   **Semantic Layer (AI/BI Readiness):** Every column is registered in Unity Catalog with a business-descriptive `COMMENT` to enable **AI/BI Genie** to perform natural language reasoning.
*   **Performance Optimization:** 
    *   **Z-Ordering:** Tables are Z-Ordered by high-cardinality join keys (`iso_code`, `date`, `h3_cell_index`) for sub-second dashboard performance.
    *   **Predictive Partitioning:** Annual tables are partitioned by `year` to optimize time-series lookups.
*   **Observability:** Every transformation cycle triggers a log entry in `gold_audit_log` capturing run-time, row-count variance, and success/fail status.

---

## Story 1: Energy & Climate Sensitivity

### 1. Objective
Quantify the relationship between daily climatic stress and national energy consumption to identify grid sensitivity to temperature volatility.

### 2. Analytical Grain
*   **Fact Table:** 1 row per `iso_code` per `date`.
*   **Reference Table:** 1 row per `iso_code` per `year`.

### 3. Engineering & Transformation Logic
1.  **Broadcast Join:** Annual electricity consumption from `silver.energy_metrics` is broadcasted across daily weather records on `iso_code` and `year` to align mismatched temporal grains.
2.  **Demand Sensitivity Index (DSI):** A calculated intensity metric:
    `DSI = (hdd + cdd) / (annual_electricity_consumption / 365)`
    *Logic: Normalizes thermal demand (Heating/Cooling Degree Days) against the average daily baseline of the national grid.*
3. **Climatological Normalization:** To account for geographic diversity, "Extreme Heat" is defined relatively. We calculate the temp_anomaly by comparing the daily mean against a 10-year monthly baseline for each iso_code. This ensures that energy demand spikes are analyzed in the context of local adaptation (e.g., a 25°C day in a typically 15°C region is flagged as a high-stress event). Heat stress is calculated using local monthly baselines to account for geographic adaptation, following EU/WMO standards for relative anomalies.

### 4. Key Business Metrics
*   **Grid Volatility:** Variance of the DSI identifying countries with unhedged climate risk.
*   **Renewable Correlation:** Statistical relationship between `avg_temperature_c` and `renewables_percentage`.

---

## Story 2: Forestry & Physical Climate Risk (PCR)

### 1. Objective
Quantify the impact of physical climate stress on carbon sequestration and forest health using spatial hierarchical indexing.

### 2. Analytical Grain
*   **Fact Table:** 1 row per `h3_cell_index` (Resolution 6) per `year`.

### 3. Engineering & Spatial Logic (The H3 Framework)
1.  **Indexing:** Utilizes **Uber H3 (Resolution 6)** (~737 km² per cell).
2.  **Spatial Reasoning:** 
    *   **Efficiency:** Replaces $O(n^2)$ Point-in-Polygon joins with $O(1)$ string-matching.
    *   **Area-Correctness:** Hexagons maintain equal area across latitudes, preventing spatial distortion in Boreal and Tropical forest analysis.
3.  **Climate Aggregation:** Daily weather is aggregated into annual counts of `extreme_heat_days` (temp > 30°C) and `drought_index` (consecutive days with no precipitation).
4.  **Carbon Sequestration Efficiency (CSE):** Calculated as `Net_Carbon_Flux / Forest_Area`.

### 4. Key Business Metrics
*   **Carbon-at-Risk:** Total flux in cells where `extreme_heat_days` exceeds the 90th percentile.
*   **Thermal Anomaly:** Variance between the current year `temp_mean` and the 10-year `ref_thermal_baselines`.

---

## Story 3: Ground Truth Verification (DQ Audit)

### 1. Objective
Validate the reliability of "Virtual" weather data (Open-Meteo Reanalysis) against "Ground Truth" physical observations (NOAA GSOD) to establish platform trust.

### 2. Analytical Grain
*   **Fact Table:** 1 row per `station_id` per `date`.

### 3. Engineering & Validation Logic
1.  **Coordinate Alignment:** Employs the **Haversine Formula** (calculated in Silver) to link each physical NOAA station to the nearest modeled grid point.
2.  **Fidelity Metrics:**
    *   **Mean Bias Error (MBE):** `(modeled_temp - observed_temp)`. Identifies if the API systematically over/underestimates local heat.
    *   **Root Mean Square Error (RMSE):** Calculated at month-grain to penalize large outliers and measure total model deviation.
3.  **Data Quality Filtering:** Exclusion of days where physical sensors report NULL or "Missing" flags to prevent skewing the accuracy score.

### 4. Key Business Metrics
*   **Model Confidence Score:** A 0–100% index derived from the inverse of the RMSE.
*   **Regional Bias Identification:** Geospatial mapping of stations with a consistent `bias_error > 2.0°C`.

---

## 4. Observability & Data Governance

### 1. Audit Table Schema (`gold_audit_log`)
| Column | Description |
| :--- | :--- |
| `run_id` | Unique UUID for the execution batch. |
| `target_table` | Name of the Gold table being updated. |
| `source_row_count` | Number of rows read from the Silver layer. |
| `target_row_count` | Number of rows written to the Gold layer. |
| `execution_status` | SUCCESS / FAILED. |
| `latency_seconds` | Time taken for the Python transformation function to complete. |

### 2. Semantic Mapping
The Gold orchestrator is responsible for deploying the **STTM (Source-to-Target Mapping)** logic. All resulting tables are enforced with `ISO-3166` (Country), `Metric` (Units), and `UTF-8` (Encoding) standards.