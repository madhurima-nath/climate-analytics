# Silver Layer: Design & Engineering Logic

This document details the engineering decisions, physical thresholds, and architectural choices used to harmonize the climate, energy, and forestry datasets. 

**Purpose:** Engineering reasoning, specific thresholds, and spatial logic.

## 1. Key Logic & Reasoning

### Climate Stress Metrics (Degree Days)
To model the energy demand required for climate control, we implement a "Neutral Band" approach:
*   **Heating (Base 15°C):** Triggered when the daily mean temperature is below 15°C. Logic: `MAX(0, 15 - temp_mean)`. 15°C is the standard residential heating activation threshold in the EU.
*   **Cooling (Base 25°C):** Triggered when the daily mean temperature exceeds 25°C. Logic: `MAX(0, temp_mean - 25)`. 25°C represents the threshold where mechanical cooling (AC) demand typically begins to scale.
*   **The Neutral Zone:** Temperatures between 16°C and 24°C result in 0 degree days, reflecting the "comfort zone" where buildings require minimal energy for temperature regulation.

### Time-Series Continuity (Forward Fill)
*   **Logic:** Forward-fill missing temperature observations in `weather_historical` for a maximum of 3 consecutive days.
*   **Reasoning:** Weather exhibits high persistence day-to-day. A short-term dropout (e.g., sensor maintenance) is best mitigated by carrying forward the last known value. Gaps longer than 3 days are left as NULL to prevent the introduction of synthetic trends.

### Spatial Identity (H3 Indexing)
*   **Resolution:** Uber H3 Resolution 6 (~737 km² per cell).
*   **Logic:** Convert coordinate-based weather data and polygon-based land use maps into a common H3 hexagonal grid.
*   **Reasoning:** H3 indexing enables high-performance joins ($O(1)$ complexity). This allows the platform to join forestry carbon flux data with historical temperature drivers without expensive "Point-in-Polygon" spatial operations.

### Normalization (Wide-to-Long)
*   **Logic:** Unpivot year-columns (e.g., `Y2010`, `Y2011`) from FAO and OWID datasets into a single `year` column.
*   **Reasoning:** Long-format data is required for efficient Spark processing and is the prerequisite for joining annual socio-economic metrics with daily weather time-series.

## 2. Implementation Framework
*   **Modular Asset Bundles (DABs):** By separating configuration (YAML) from transformation logic (Python), the system can scale to new countries or metrics without modifying core code.
*   **Functional Library:** All transformations are written as atomic, testable Python functions within domain-specific modules (`weather.py`, `nature.py`, etc.).
*   **Metric Standards:** All data is standardized to Metric units (Celsius, Millimeters, Hectares) and energy metrics to Terawatt-hours (TWh) to ensure a single source of truth for the Gold layer.

## 3. Data Health Requirements
The pipeline enforces the following validation checks:
1. `temp_min <= temp_max`
2. `forest_area <= total_country_land_area`
3. 100% compliance with ISO-3166 Alpha-3 country codes.
