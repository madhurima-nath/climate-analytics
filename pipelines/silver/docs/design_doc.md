# Silver Layer: Design & Engineering Logic

This document details the engineering decisions and physical thresholds used to harmonise climate, energy, and forestry datasets.

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

## 3. Data Persistence (Forward Fill)
*   **Logic:** Missing temperature observations are forward-filled for a maximum of 3 consecutive days.
*   **Reasoning:** Weather exhibits high persistence. Short-term sensor dropouts are best mitigated by carrying forward the last known value. Gaps exceeding 3 days remain as NULL to prevent the introduction of synthetic trends.