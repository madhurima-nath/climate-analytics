# Silver Layer Design & Engineering Logic

This document details the engineering decisions, physical thresholds, and reasoning used to standardize the climate and energy datasets.

## 1. Key Logic & Reasoning

### Climate Stress (Degree Days)
We use a "Neutral Band" approach to accurately model when energy is needed for heating or cooling:
*   **Heating (Base 15°C):** Calculated as `MAX(0, 15 - mean_temp)`. 15°C is the standard activation threshold for residential heating in the EU.
*   **Cooling (Base 25°C):** Calculated as `MAX(0, mean_temp - 25)`. 25°C is the threshold where air conditioning demand typically begins to scale.
*   **Reasoning:** Temperatures between 16°C and 24°C result in 0 degree days. This prevents the error of overlapping demand and reflects the "comfort zone" where buildings require minimal climate control.

### Gap Handling (Persistence)
*   **Rule:** Missing temperature data is "Forward Filled" for a maximum of 3 consecutive days.
*   **Reasoning:** Weather exhibits "persistence." Yesterday's temperature is a more reliable proxy than a long-term average for short sensor outages. Gaps longer than 3 days are left as NULL to avoid creating false trends.

### Spatial Identity (H3 Indexing)
*   **Resolution:** H3 Resolution 6 (~737 km² per cell).
*   **Logic:** We convert forestry polygons and climate coordinates into H3 hexagons.
*   **Reasoning:** Joining by H3 index is significantly faster than "Point-in-Polygon" spatial joins. Resolution 6 is granular enough to distinguish urban areas from forests while remaining performant on a global scale.

### Data Normalization
*   **Unpivoting:** FAO and OWID data are unpivoted from "Year-per-Column" to "Year-per-Row." This is essential for joining annual metrics with daily weather time-series.
*   **Metric Standards:** NOAA data is converted from Imperial (Fahrenheit/Inches) to Metric (Celsius/mm) to ensure a single standard across the platform.

## 2. Engineering Standards
*   **DABs (Asset Bundles):** Ensures the project follows Infrastructure-as-Code (IaC) principles and is easy to deploy across environments.
*   **Function Library:** All logic is written as reusable Python functions. This makes the code testable and ensures that a change to a calculation (like a unit conversion) applies consistently to all tables.
*   **Scalability:** By driving ingestion through `reference_locations.csv` and transformations through YAML, the system can add new countries or metrics without modifying the core Python code.

## 3. Data Health Requirements
Tables are only updated if they pass these logic checks:
1. `temp_min <= temp_max`
2. `forest_area <= total_land_area`
3. Valid ISO-3166 Alpha-3 country code present.