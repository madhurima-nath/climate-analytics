# Bronze Layer: Global Ingestion & Raw Landing Zone

This directory contains the suite of notebooks responsible for the ingestion of global climate, energy, and environmental datasets into the `climate_energy_demand.bronze` schema.

## 1. Architectural Philosophy
The Bronze layer is designed as a **Raw Mirror** of the source systems.
*   **1-to-1 Mapping:** Every source file or API response is stored in a corresponding Delta table to preserve data lineage.
*   **Immutability:** No cleaning, filtering, or unit conversions are performed here. All raw "quirks" (e.g., Fahrenheit units, non-standard headers, and API-specific flags) are preserved.
*   **Lineage Enrichment:** Every record is enriched with `ingested_at` (timestamp) and `source_file` (metadata path/URL) to ensure 100% traceability.
*   **Global Scaling:** Ingestion is **data-driven** via `reference_locations.csv`, allowing the pipeline to scale from a few regions to 250+ global territories without code modification.

## 2. Infrastructure Constraints & Workarounds
### Unity Catalog Volumes (Egress Management)
Due to network egress restrictions in the Databricks Free Edition, several datasets (OWID, FAOSTAT, GFW, NOAA) are ingested via a **Manual-to-Volume** pattern. 
1.  Data is downloaded locally.
2.  Uploaded to `/Volumes/climate_energy_demand/bronze/raw_uploads/`.
3.  Ingested into Delta tables via Spark to ensure they are governed by Unity Catalog.

    - **Sequential Batch Persistence (Timeout Management)** To mitigate the 10-minute execution timeout enforced by the Databricks Free Edition, Notebook 05 (CMIP6) implements a **Sequential Transactional Commit** pattern. Data is persisted to Delta Lake immediately following the completion of every 50-location batch. This ensures that if the environment terminates, all prior progress is hard-written to disk and the pipeline can resume instantly without data loss.

## 3. Data Sources

| Notebook | Dataset | Provider | Grain |
| :--- | :--- | :--- | :--- |
| **01** | Historical Weather | Open-Meteo Archive | Daily (2010–Present) |
| **02** | Incremental Weather | Open-Meteo Forecast | Daily (Recent Updates) |
| **03** | Energy Consumption | Our World in Data (OWID) | Annual (Global) |
| **04** | Ground-Truth Observations | NOAA GSOD | Daily (Station-level) |
| **05** | Climate Projections | Open-Meteo CMIP6 | Daily (7 Models, Sample Years) |
| **06** | Forestry & Land Cover | FAOSTAT (UN) | Annual (Relational) |
| **07** | Carbon Flux & Spatial | Global Forest Watch | Annual / Spatial Polygons |

## 4. Engineering Decisions & Resilience

### A. API Resilience (Throttling & Backoff)
To handle the high volume of requests for 250+ locations, the Open-Meteo notebooks (01, 02, 05) implement a **Fault-Tolerant Retry Logic**:
*   **Exponential Backoff:** If an API returns a `429 (Too Many Requests)` error, the script enters a 5-minute cooldown period before retrying.
*   **Throttling:** 
    *   **Backfills:** 2.0s delay per request to accommodate large data payloads (15 years of daily stats).
    *   **Incremental:** 1.5s delay for lighter daily updates.
*   **Randomized Jitter:** Small variations in sleep time are used to prevent "robotic" request patterns.

### B. Fault-Tolerant Checkpointing
Ingestion notebooks for weather and stations utilize an **Append-on-Success** pattern. 
* Granular Tracking: State is tracked at the (Country, Year) grain, identifies completed countries/years already present in the Delta table. The pipeline performs a pre-flight metadata scan of the Delta table to generate a work-queue of missing tasks. It automatically filters the ingestion queue to skip completed work.
* Idempotency: This ensures that in the event of a network failure or daily API limit, the pipeline can be resumed immediately without data loss or redundant processing.

### C. Geospatial Precision (Haversine)
Notebook **04 (NOAA)** utilizes the **Haversine Formula** to find the nearest physical weather station to each country's centroid. 
*   Unlike Euclidean distance, Haversine accounts for the Earth's curvature. 
*   This is essential for accurate station selection in high-latitude regions (e.g., the Nordics) where longitudinal convergence distorts standard map-based distance calculations.

### D. FAO Relational Ingestion
FAOSTAT data is ingested in its raw "E_All_Data" format. We ingest 5 files per domain (Data, AreaCodes, ItemCodes, Elements, Flags).
*   **Choice of `All_Data`:** We prioritize the "Long" format over the "NOFLAG" version to ensure that data quality markers (e.g., "Official Data" vs. "Estimate") are available for downstream Silver-layer validation.

## 5. Maintenance & Refresh
*   **Weather:** Notebook 02 is designed for daily execution.
*   **Energy/Forestry:** Update the source CSVs in the Volume annually and rerun the respective ingestion notebooks.
*   **Stations:** NOAA GSOD is a static backfill (Source cutoff: Aug 2025).

* **Probabilistic Ensemble & Strategic Sampling** (Notebook 05)
To support EU-level climate energy demand modeling, the CMIP6 ingestion logic follows a specific scientific methodology:

- Uncertainty Signaling: The pipeline ingests all 7 available CMIP6 HighResMIP models (e.g., EC-Earth3P-HR, MRI-AGCM3-2-S). The variance across this ensemble provides the necessary uncertainty signal required for risk modeling in the absence of SSP scenario selectors.
- Strategic Temporal Sampling: Rather than a continuous 100-year pull, we ingest complete calendar years in 5-year intervals (2020–2050). This provides sufficient longitudinal resolution for annual Heating/Cooling Degree Day (HDD/CDD) calculations while reducing API overhead by 80%.
- Vectorized Tidy Transformation: Uses Pandas melt and pivot within the ingestion loop to reshape wide API responses into a "Tidy Data" format (Model-as-a-column), facilitating direct SQL-based ensemble analysis.

---
*Note: This layer provides the high-fidelity raw foundation required for the subsequent Trusted (Silver) layer transformations.*