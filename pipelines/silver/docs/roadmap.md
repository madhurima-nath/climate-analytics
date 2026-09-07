# Project Roadmap & Tier Scaling

## Evolution Path

```
┌──────────────────────────────────────────────────────────────────┐
│                    Current: Community Edition                    │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Serverless (auto-selected)                         │
│  Trigger:     Manual / DAB deploy                                │
│  State:       Delta table watermarks                             │
│  Spatial:     H3 Resolution 6 (~737 km²)                         │
│  Testing:     pytest unit tests (45 tests)                       │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             │ Upgrade
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              Phase 1: Standard Edition (Paid Tier)               │
├──────────────────────────────────────────────────────────────────┤
│  Compute:     Job clusters (optimized sizing)                    │
│  Trigger:     Scheduled jobs (cron) + File arrival               │
│  Ingestion:   Auto Loader (cloud storage events)                 │
│  State:       Delta table watermarks + Auto Loader checkpoints   │
│  Spatial:     H3 Resolution 6 → 7 (~137 km²)                     │
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

## Current State: Databricks Community/Standard Edition
*   **Compute:** Serverless compute (auto-selected).
*   **Ingestion:** DAB-orchestrated batch processing with watermark-based incremental loads.
*   **Pipeline:** Job-based orchestration defined in `databricks.yml`:
    *   `initialise_silver_infrastructure` (SQL task)
    *   `run_silver_orchestrator` (notebook task)
    *   `validate_silver_tables` (Python file task)
*   **Audit:** Delta table `climate_energy_demand.silver.ingestion_audit` tracks watermarks and row counts.
*   **Logic:** H3 Resolution 6 to balance performance and regional precision.

## Future Phase: Production (Paid) Tier
1.  **Automated Ingestion:** Transition to **Auto Loader** or file-arrival triggers. Pipelines will automatically run as files arrive in cloud storage.
2.  **Increased Precision:** Scale H3 Indexing to Resolution 8 or 9 (~1 km²) for urban heat island analysis.
3.  **Real-Time Monitoring:** Implement **Lakeflow Spark Declarative Pipelines (SDP)** for continuous data quality monitoring and automated lineage tracking.
4.  **Precipitation Integration:** Full incorporation of global rainfall data (e.g., ERA5-Land) once a reliable Bronze source is established.
5.  **Enhanced Testing:** Expand unit test coverage beyond Silver validation to include transform function tests and integration tests.