# Climate, Energy & Nature: Silver Layer

This directory contains the engineering logic for the **Silver layer** of the data platform. Silver is the standardisation gate: every dataset from the FAO, OWID, NOAA, and Open-Meteo arrives in Bronze with different units, formats, and quirks, and leaves Silver clean, metric, and query-ready. Every downstream table, dashboard, and Genie answer depends on this layer being right.

## Core Objectives
1. **Standardisation:** Transforming raw heterogeneous Bronze datasets into consistent, metric-standardised Delta tables.
2. **Geospatial Alignment:** Bridging the gap between coordinate-based weather data and area-based forestry metrics.

## Architecture
A single orchestrator notebook runs every Silver table. This centralised model is deliberate: one place enforces logging, error handling, and write patterns, so every table behaves the same way regardless of who added it.

``` mermaid
graph TD
    A[Bronze Delta Tables] --> B[Silver Orchestrator]
    C[YAML Pipeline Configs] --> B
    D[Python Transform Modules] --> B
    B --> E[Silver Delta Tables]
    B --> F[Operational Metadata / Audit]
    G[AI/BI Genie Space] -.-> E
```

## Repository Structure
``` text
├── pipelines/
│   ├── silver/
│   │   ├── configs/                # YAML declarative pipeline definitions for the silver tables
│   │   │   ├── energy_metrics.yml
│   │   │   ├── weather_historical.yml
│   │   │   ├── weather_observations.yml
│   │   │   ├── weather_projections.yml
│   │   │   ├── forest_inventory_annual.yml
│   │   │   ├── land_cover_annual.yml
│   │   │   ├── temp_change_annual.yml
│   │   │   ├── dim_stations.yml
│   │   │   ├── dim_date.yml
│   │   │   └── dim_locations.yml
│   │   ├── docs/                    # documentation for design, architectural decisions and roadmap
│   │   ├── silver_orchestrator   # centralised execution engine (notebook)
│   │   ├── silver_sdp.py         # SDP source file (declarative silver tables)
│   │   └── setup_silver.sql        # audit schema initialisation
│   └── consumption/
│       └── monitoring/             # Monitoring tables & dashboard design doc
├── src/
│   ├── common/                    # shared utilities and audit logic
│   ├── transforms/                # domain-specific transformation logic
│   └── tests/                    # pytest suites (all layers)
└── databricks.yml                 # Declarative Automation Bundle manifest
```

## Implementation Logic
### Orchestrator
The `silver_orchestrator` is the single execution engine for all Silver tables. It contains no transformation logic itself. For each YAML config it runs the same pattern: read the config, check the watermark, extract from Bronze, call the transform function, MERGE into the target, and update the audit log. Because every table passes through this one loop, logging, error handling, and write behaviour are identical across the project, and a failure in one table never stops the others.

The full per-config flow, with diagrams, is in [design_doc.md](docs/design_doc.md).

### Declarative Configuration (YAML)
Each Silver table is declared in a YAML config that specifies its sources, transform module and function, merge keys, and watermark column. The intent lives in the YAML; the implementation lives in Python.
* **Decoupling**: Changes to sources or target paths need no Python changes.
* **Extensibility**: Adding a table means adding a YAML file; the orchestrator discovers and processes it automatically.
* **Auditability**: The configs are version-controlled and provide a human-readable map of data lineage.

### Incremental State Management (Watermarks)
Watermarks keep the Silver loads incremental, which matters on Free Edition where compute is limited:
* **Incremental Mode**: When a config defines a `watermark_column`, the orchestrator reads only Bronze records newer than the last processed watermark, so nothing is reprocessed. The state lives in the queryable `silver.ingestion_audit` table (ADR #4), not in opaque checkpoint files.
* **Full Refresh**: Without a watermark, the table is rebuilt completely. This is used for reference datasets and historical corrections.

### Monitoring and AI/BI Integration
Operational health is managed through the `consumption/monitoring/` layer.
* **AI/BI Dashboards**: Real-time visibility into pipeline runs
* **Genie Capabilities**: The Silver tables are configured with rich metadata to support natural language querying (Genie). This allows stakeholders to ask questions in natural language without manual SQL intervention.


## Deployment
This project is deployed as a Declarative Automation Bundle.

**Pipeline Approaches**:
The Silver layer supports two coexisting orchestration approaches, both available on Free Edition:
* **Notebook Orchestration:** The `silver_orchestrator` notebook processes all YAML configs in a single run. This is the existing approach, deployed as the `silver_data_load` job.
* **Declarative Pipelines (SDP):** Silver tables can also be declared as `@dlt.streaming_table` in an SDP source file, with automatic checkpointing replacing the manual watermark system. Limited to one active pipeline per type on Free Edition.

Both approaches share the same `src/transforms/*.py` modules. For a full comparison and the coexistence exit criteria, see [Pipeline Approaches](../../pipeline_approaches.md).

**Environment Constraints**:
The current architecture is developed for the Databricks Free Edition. In this version:
* Serverless compute is used for all jobs (no cluster configuration needed).
* Both notebook-based jobs and SDP pipelines are supported, with concurrency limits (5 concurrent job tasks, 1 active SDP pipeline per type).
* The transition path to the Paid/Enterprise tier is documented in `silver/docs/roadmap.md`.
