# Gold Layer Architecture: Climate & Energy Intelligence

The Gold layer is the final "Consumption Zone" of the platform. It denormalises Silver-layer data into high-value, semantic tables optimised for Physical Climate Risk (PCR) analysis, AI/BI Genie discovery, and executive dashboards.

## 1. Project Structure

```
gold/
├── configs/                          # YAML-driven table definitions
│   ├── fct_energy_demand.yml        # Daily energy demand + weather sensitivity
│   ├── fct_temp_change_annual.yml  # Annual temperature change metrics
│   ├── fct_land_cover_annual.yml   # Annual land cover statistics
│   ├── fct_forest_resilience.yml    # Annual forest carbon + climate stress (country grain)
│   ├── fct_ground_truth_audit.yml   # Weather model validation metrics
│   ├── dim_stations.yml             # Station dimension
│   ├── dim_locations.yml            # Location/country dimension
│   ├── dim_koppen_zones.yml         # Köppen-Geiger climate zone dimension
│   └── dim_date.yml                 # Date dimension
│
├── docs/                             # Technical documentation
│   ├── design_doc.md                # Detailed engineering design patterns
│   └── architecture_logic.md        # Business logic and metric definitions
│
├── source_to_target_mappings/       # Lineage documentation (CSV per table plus index)
│   └── gold_source_to_target_mappings.md  # See index for full list
│
├── setup_gold.sql                   # Creates gold.ingestion_audit table
├── gold_orchestrator.py             # Config-driven transformation runner (notebook)
└── README.md                        # This file
```

**Key Design Principles:**
* **Config-Driven**: All table schemas, joins, and metadata defined in YAML
* **Modular**: Each fact table has its own config + transformation function
* **Observable**: Every run logs to `gold.ingestion_audit` for lineage tracking
* **AI-Ready**: Column comments auto-sync to Unity Catalog for Genie discovery

## 2. System Wiring (The Registry)
The `gold_orchestrator.py` dynamically maps Gold tables to their specific transformation modules based on the YAML definitions.

| Target Table | Configuration File | Python Module | Transformation Function |
| :--- | :--- | :--- | :--- |
| `fct_energy_demand_daily` | `fct_energy_demand.yml` | `src.transforms.energy` | `process_energy_demand` |
| `fct_temp_change_annual` | `fct_temp_change_annual.yml` | `src.transforms.nature` | `process_temp_change_gold` |
| `fct_land_cover_annual` | `fct_land_cover_annual.yml` | `src.transforms.nature` | `process_land_cover_gold` |
| `fct_forest_resilience_annual` | `fct_forest_resilience.yml` | `src.transforms.nature` | `process_forest_resilience` |
| `fct_ground_truth_verification_daily` | `fct_ground_truth_audit.yml` | `src.transforms.quality` | `process_fidelity_audit` |
| `dim_stations` | `dim_stations.yml` | `src.transforms.climate` | `transform_gold_dim_stations` |
| `dim_locations` | `dim_locations.yml` | `src.transforms.climate` | `transform_gold_dim_locations` |
| `dim_date` | `dim_date.yml` | `src.transforms.climate` | `transform_gold_dim_date` |
| `dim_koppen_zones` | `dim_koppen_zones.yml` | `src.transforms.climate` | `transform_dim_koppen_zones` |

## 3. Engineering Foundations & Observability

### A. Semantic AI-Readiness
Every YAML configuration includes a `comments` attribute for all columns. The Orchestrator automatically pushes these descriptions to Unity Catalog, enabling the **AI/BI Genie** to interpret natural language queries (e.g., *"Which countries have high heating sensitivity?"*) without additional manual documentation. Thermal stress is measured as a climatological anomaly (departure from local normal) rather than a fixed global temperature.

### B. Spatial Alignment
Forest resilience data is at country-year grain, joining FAO forest inventory (country grain) with Open-Meteo weather aggregates (country grain) via `dim_locations` for ISO code resolution.

### C. Audit & Transactional Logging
The orchestrator records every transformation cycle into `climate_energy_demand.gold.ingestion_audit`. This ensures the pipeline is fully observable. Each entry captures:
*   **Pipeline Lineage:** Run IDs, table names, and timestamps.
*   **Data Integrity:** Input vs. Output row counts to identify data loss.
*   **Performance:** Latency per transformation module for compute optimisation.

Details:
* `run_id`: Unique UUID for the execution batch.
* `target_table`: The specific gold table being processed.
* `row_counts`: Input (Silver) vs. Output (Gold) counts to monitor data drift/loss.
* `execution_metrics`: Start time, end time, and duration in seconds.
* `error_message`: Captured tracebacks in the event of a transformation failure.
