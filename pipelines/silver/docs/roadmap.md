# Project Roadmap & Tier Scaling

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