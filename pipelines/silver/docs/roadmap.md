# Project Roadmap & Tier Scaling

## Evolution Path

```
┌──────────────────────────────────────────────────────────────────┐
│                    Current: Free Edition                         │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Serverless (auto-selected)                         │
│  Trigger:     Manual / DAB deploy                                │
│  State:       Delta table watermarks                             │
│  Spatial:     Grid-based indexing (~33km cells)                │
│  Testing:     pytest unit tests (all layers)                    │
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
│  Spatial:     H3 Resolution 7 (~137 km²) — requires H3 library    │
│  Testing:     Unit + Integration tests                           │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             │ Scale Up
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│          Phase 2: Premium Edition (Production Scale)             │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Multi-node clusters + Photon acceleration          │
│  Trigger:     Event-driven (table updates, file arrival)         │
│  Pipeline:    Lakeflow Spark Declarative Pipelines (SDP)         │
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
*   **Pipeline:** Job-based orchestration defined in `databricks.yml`:
    *   `project_bootstrap` (one-time SQL task — creates catalog, schemas, audit tables)
    *   `bronze_validation` (notebook task — runs pytest suite)
    *   `silver_data_load` (notebook task — runs silver orchestrator)
    *   `silver_validation` (notebook task — runs pytest suite)
    *   `gold_data_load` (notebook task — runs gold orchestrator)
    *   `gold_validation` (notebook task — runs pytest suite)
    *   `climate_data_pipeline` (full end-to-end: bronze validate → silver load → silver validate → gold load → gold validate)
*   **Audit:** Delta table `climate_energy_demand.silver.ingestion_audit` tracks watermarks and row counts.
*   **Logic:** Grid-based spatial indexing (~33km cells) to balance performance and regional precision. H3 hexagonal indexing is planned for the Paid Tier (requires the H3 library on job clusters).

## Future Phase: Production (Paid) Tier
1.  **Automated Ingestion:** Transition to **Auto Loader** or file-arrival triggers. Pipelines will automatically run as files arrive in cloud storage.
2.  **Increased Precision:** Scale H3 Indexing to Resolution 8 or 9 (~1 km²) for urban heat island analysis.
3.  **Real-Time Monitoring:** Implement **Lakeflow Spark Declarative Pipelines (SDP)** for continuous data quality monitoring and automated lineage tracking.
4.  **Precipitation Integration:** Full incorporation of global rainfall data (e.g., ERA5-Land) once a reliable Bronze source is established.
5.  **Enhanced Testing:** Add end-to-end integration tests (layer-specific pytest suites already cover bronze, silver, and gold validation).