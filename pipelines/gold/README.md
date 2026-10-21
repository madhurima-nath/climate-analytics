# Gold Layer Architecture: Climate & Energy Intelligence

The Gold layer is the consumption zone of the platform. It denormalises Silver data into fact and dimension tables that answer specific analytical questions directly, without the reader having to join or aggregate anything. Each table targets one use case: Physical Climate Risk (PCR) analysis, AI/BI Genie discovery, or executive dashboards.

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
├── gold_orchestrator             # Config-driven transformation runner (notebook)
├── gold_sdp.py                   # SDP source file (declarative gold materialized views)
└── README.md                        # This file
```

**Key Design Principles:**
* **Config-Driven**: All table schemas, joins, and metadata are defined in YAML, so adding a fact table means a new config, not new orchestration code.
* **Modular**: Each fact table has its own config and transformation function, so a failure in one table does not block the others.
* **Observable**: Every run logs to `gold.ingestion_audit` for lineage tracking.
* **AI-Ready**: Column comments auto-sync to Unity Catalog for Genie discovery.

## Pipeline Approaches

The Gold layer supports two coexisting orchestration approaches, both available on Free Edition:
* **Notebook Orchestration (existing):** The `gold_orchestrator` notebook processes all YAML configs, performing full-reload (overwrite) on each run. Gold tables are derived aggregates of Silver, so a full rebuild is simpler and safer than incremental upserts: there is no merge state to drift, and Silver remains the single source of truth. Monitoring writes to `climate_energy_demand.monitoring.pipeline_runs`.
* **Declarative Pipelines (SDP):** Gold tables can be declared as `@dlt.materialized_view` in an SDP source file, with automatic dependency resolution on upstream Silver streaming tables. Monitoring uses the SDP event log.

Both approaches share the same `src/transforms/*.py` modules. For a full comparison and the coexistence exit criteria, see [Pipeline Approaches](../../pipeline_approaches.md).

## 2. System Wiring (The Registry)
The `gold_orchestrator` maps each Gold table to its transformation module via the YAML definitions. The mapping is config-driven for the same reason as Silver (ADR #6): the orchestrator has no hard-coded table list, so adding a Gold table requires only a new YAML file and transform function.

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
