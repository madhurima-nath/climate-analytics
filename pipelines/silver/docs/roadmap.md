# Project Roadmap & Tier Scaling

## Current State: Databricks Free Edition
*   **Compute:** Single-node clusters.
*   **Ingestion:** Manual batch triggers based on `ingested_at` watermarks.
*   **Logic:** H3 Resolution 6 to balance performance and regional precision.

## Future Phase: Production (Paid) Tier
1.  **Automated Ingestion:** Transition from manual batching to **Databricks Auto Loader**. This will automatically trigger pipelines as files arrive in cloud storage.
2.  **Increased Precision:** Scaling H3 Indexing to Resolution 8 or 9 (~1 km²) for urban heat island analysis.
3.  **Real-Time Monitoring:** Implementation of **Delta Live Tables (DLT)** for continuous data quality monitoring and automated lineage tracking.
4.  **Precipitation Integration:** Full incorporation of global rainfall data (e.g., ERA5-Land) once a reliable Bronze source is established.