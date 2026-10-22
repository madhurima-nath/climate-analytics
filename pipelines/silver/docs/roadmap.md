# Project Roadmap & Tier Scaling

## Evolution Path

```
┌──────────────────────────────────────────────────────────────────┐
│                    Current: Free Edition                         │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Serverless (auto-selected)                         │
│  Orchestration: Two coexisting approaches:                       │
│               A) Notebook jobs (silver/gold orchestrators)        │
│               B) Lakeflow Spark Declarative Pipelines (SDP)      │
│                  (1 active pipeline per type, Free Ed. limit)   │
│  Trigger:     Manual / DAB deploy                                │
│  State:       A) Delta table watermarks (ingestion_audit)       │
│               B) SDP automatic checkpoints                       │
│  Spatial:     Grid-based indexing (~33km cells)                │
│  Testing:     pytest unit tests (all layers)                    │
│  Monitoring:  A) pipeline_runs custom table                     │
│               B) SDP event log (event_log())                     │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             │ Upgrade
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              Phase 1: Standard Edition (Paid Tier)               │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Job clusters (optimised sizing)                    │
│  Trigger:     Scheduled jobs (cron) + File arrival               │
│  Ingestion:   Auto Loader (cloud storage events)                 │
│  State:       Delta table watermarks + Auto Loader checkpoints   │
│  Spatial:     H3 Resolution 7 (~137 km²), requires H3 library     │
│  Testing:     Unit + Integration tests                           │
│  SDP:         Multiple active pipelines (no per-type limit)      │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             │ Scale Up
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│          Phase 2: Premium Edition (Production Scale)             │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Multi-node clusters + Photon acceleration          │
│  Trigger:     Event-driven (table updates, file arrival)         │
│  State:       SDP streaming checkpoints + DQM expectations       │
│  Spatial:     H3 Resolution 8-9 (~1-5 km²) for urban analysis   │
│  Data:        + Precipitation (ERA5-Land)                        │
│  Monitoring:  Data Quality Monitoring (DQM) + Alerts             │
│  Testing:     Unit + Integration + E2E + Performance             │
└──────────────────────────────────────────────────────────────────┘
```

## Current State: Databricks Free Edition
*   **Compute:** Serverless compute (auto-selected).
*   **Ingestion:** DAB-orchestrated batch processing with watermark-based incremental loads.
*   **Pipeline, two coexisting approaches:**
    *   **Approach A (Notebook Orchestration):** Job-based orchestration defined in `databricks.yml`:
        *   `project_bootstrap` (one-time SQL task that creates the catalog, schemas, and audit tables)
        *   `bronze_validation` (notebook task that runs the pytest suite)
        *   `silver_data_load` (notebook task that runs the silver orchestrator)
        *   `silver_validation` (notebook task that runs the pytest suite)
        *   `gold_data_load` (notebook task that runs the gold orchestrator)
        *   `gold_validation` (notebook task that runs the pytest suite)
        *   `climate_data_pipeline` (full end-to-end: bronze validate → silver load → silver validate → gold load → gold validate)
    *   **Approach B (SDP):** A single Lakeflow Spark Declarative Pipeline combining Silver streaming tables (with `weather_historical` as a materialized view, ADR #9) and Gold materialized views. Supported on Free Edition with a limit of one active pipeline per pipeline type. First full update completed and row-count parity validated 21 October 2026.
*   Both approaches share the same `src/transforms/*.py` modules and read from the same Bronze tables.
*   For a detailed comparison, see [Pipeline Approaches](../../../pipeline_approaches.md).
*   **Audit:** Delta table `climate_energy_demand.silver.ingestion_audit` tracks watermarks and row counts (Approach A). SDP uses automatic checkpointing (Approach B).
*   **Logic:** Grid-based spatial indexing (~33km cells) to balance performance and regional precision. H3 hexagonal indexing is planned for the Paid Tier (requires the H3 library on job clusters).

## Future Phase: Production (Paid) Tier
1.  **Automated Ingestion:** Transition to **Auto Loader** or file-arrival triggers. Pipelines will automatically run as files arrive in cloud storage.
2.  **Multiple SDP Pipelines:** Paid tier removes the one-active-pipeline-per-type limit, allowing separate Silver and Gold SDP pipelines with independent scheduling and failure isolation.
3.  **Increased Precision:** Scale H3 Indexing to Resolution 8 or 9 (~1 km²) for urban heat island analysis.
4.  **Real-Time Monitoring:** Implement **Data Quality Monitoring (DQM)** for continuous data quality monitoring and automated lineage tracking.
5.  **Precipitation Integration:** Full incorporation of global rainfall data (e.g., ERA5-Land) once a reliable Bronze source is established.
6.  **Enhanced Testing:** Add end-to-end integration tests (layer-specific pytest suites already cover bronze, silver, and gold validation).