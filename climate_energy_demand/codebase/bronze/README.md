# Bronze Layer: Global Ingestion & Raw Landing Zone

This directory contains the suite of notebooks responsible for the ingestion of global climate, energy, and environmental datasets into the `climate_energy_demand.bronze` schema.


## 1. Design Principle
The Bronze layer is architected as a **Raw Mirror** of the source systems to ensure full auditability:
*   **1-to-1 Mapping:** Every source file or API response is stored in a corresponding Delta table to preserve data lineage.
*   **Immutability:** Data is persisted in its native format. No cleaning, filtering, or unit conversions are performed here. All raw "quirks" (e.g., Fahrenheit units, non-standard headers, and API-specific flags) are preserved.
*   **Lineage Enrichment:** Every record is enriched with `ingested_at` (timestamp) and `source_file` (metadata path/URL) to ensure 100% traceability.
*   **Global/Decoupled Scaling:** Ingestion is **data-driven** via `reference_locations.csv`, allowing the pipeline to scale from a few regions to 250+ global territories without code modification.


## 2. Infrastructure Constraints & Resiliency
The pipelines are specifically optimised for the Databricks Free Edition (2026) environment.
### Unity Catalog (UC) Volumes: Egress Management
Due to network egress restrictions in the Databricks Free Edition, several datasets (OWID, FAOSTAT, GFW, NOAA) are ingested via a **Manual-to-Volume** pattern. 
1.  Data is downloaded locally.
2.  Uploaded to `/Volumes/climate_energy_demand/bronze/raw_uploads/`.
3.  Ingested into Delta tables via Spark to ensure they are governed by Unity Catalog.

### Sequential Batch Persistence (Timeout Management)
To mitigate the 10-minute execution timeout enforced by the Databricks Free Edition, Notebook for CMIP6 data implements a **Sequential Transactional Commit** pattern. Data is written to Delta Lake immediately following the completion of every 5-location batch. This ensures that if the environment terminates, the progress is saved and the pipeline can resume instantly without data loss.


## 3. Data Sources

| Notebook | Dataset | Provider | Temporal Grain | Technical Method
| :--- | :--- | :--- | :--- | :--- |
| **01** | Historical Weather | Open-Meteo Archive | Daily (2010–Present) | API Backfill with replaceWhere |
| **02** | Incremental Weather | Open-Meteo Forecast | Daily (Recent Updates) | Delta Merge with 7-day look-back |
| **03** | Energy Consumption | Our World in Data (OWID) | Annual (Global) | Volume-to-Delta Overwrite |
| **04** | Ground-Truth Observations | NOAA GSOD | Daily (Station-level) | Haversine Spatial Discovery |
| **05** | Climate Projections | Open-Meteo CMIP6 | Daily (Sampled) | Multi-model Ensemble (7 Models)
| **06** | Forestry, Land Cover, Carbon Flux | FAOSTAT (UN) + Global Forest Watch | Annual, spatial per file | Bulk Volume Ingestion | 


## 4. Engineering Decisions & Resilience

### A. Fault-Tolerant Checkpointing
Ingestion notebooks for weather and stations utilize an **Append-on-Success** pattern. 
* Granular Tracking: State is tracked at the (Country, Year) grain, identifies completed countries/years already present in the Delta table. The pipeline performs a pre-flight metadata scan of the Delta table to generate a work-queue of missing tasks. It automatically filters the ingestion queue to skip completed work.
* Idempotency: This ensures that in the event of a network failure or daily API limit, the pipeline can be resumed immediately without data loss or redundant processing.

### B. API Resilience (Throttling & Backoff)
To handle the high volume of requests for 250+ locations, the Open-Meteo notebooks implement a **Fault-Tolerant Retry Logic**. This includes exponential backoff for 429 (Too Many Requests) errors and a 2.0s throttling delay to ensure stability during high-volume global backfills.
*   **Exponential Backoff:** If an API returns a `429 (Too Many Requests)` error, the script enters a cooldown period before retrying.
*   **Throttling:** 
    *   **Backfills:** 2.0s delay per request to accommodate large data payloads (15 years of daily stats).
    *   **Incremental:** 1.5s delay for lighter daily updates.  

### C. Geospatial Precision (Haversine)
Notebook **NOAA** implements the **Haversine Formula** to find the nearest physical weather station to each country's centroid. 
*   Unlike Euclidean distance, Haversine accounts for the Earth's curvature. 
*   This is essential for accurate station selection in high-latitude regions (e.g., the Nordics) where longitudinal convergence distorts standard map-based distance calculations.

### D. Probabilistic Climate Modelling
Notebook **CMIP6** supports the ingestion of all 7 available HighResMIP ensemble models (e.g., EC-Earth3P-HR, MRI-AGCM3-2-S). To optimise performance, the pipeline utilizes Strategic Temporal Sampling, ingesting complete years in 5-year intervals, instead of a continuous pull. This provides the necessary longitudinal resolution for Heating/Cooling Degree Day (HDD/CDD) calculations while reducing API overhead.

### E. Character Encoding & Schema Sanitisation
Notebook **FAO GFW** handles diverse international datasets from the UN (FAOSTAT) and Global Forest Watch (GFW).
* Encoding Resilience: Uses ISO-8859-1 encoding to preserve special characters in international area names.
* Automated Sanitisation: Implements a Regex-based sanitisation function to ensure all raw CSV headers are converted into Delta-compatible, lowercase, and snake-case column names.
* Dynamic Table Routing: Uses a pattern-matching engine to automatically route diverse CSV files (Land Cover, Peatlands, Carbon Flux) to their respective target tables based on filename metadata.


## 5. Maintenance & Refresh
*   **Weather:** Notebook **incremental** is designed for daily execution.
*   **Energy/Forestry/Environmental Data:** Source CSVs in the UC Volumes needs to be updated annually by executing the respective ingestion notebooks .
*   **Validation:** NOAA GSOD is a static backfill (Source cutoff: Aug 2025).

---
*Note: This layer provides the high-fidelity raw foundation required for the subsequent Silver-layer standardisation and Gold-layer analytics.*