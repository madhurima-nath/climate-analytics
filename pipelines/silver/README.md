# Climate, Energy & Nature: Silver Layer

This repository contains the engineering logic for the **Silver Layer** of the data platform. The system harmonises disparate datasets from the FAO, OWID, NOAA, and OpenMeteo into a unified, query-ready state. This layer transforms the raw, 

## Core Objectives
1. **Standardisation:** Transforming raw heterogeneous Bronze datasets into consistent, metric-standardised Delta tables.
2. **Geospatial Alignment:** Bridging the gap between coordinate-based weather data and area-based forestry metrics.

## Architecture
A centralised execution model is employed to decouple transformation logic from data orchestration.

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
│   │   ├── configs/           # YAML declarative pipeline definitions for the silver tables
│   │   ├── docs/              # documentation for design, architectural decisions and roadmap
│   │   ├── silver_orchestrator.py  # centralised execution engine
│   │   └── setup_silver.sql   # audit schema initialisation
│   └── consumption/
│       └── monitoring/        # AI/BI Dashboard & Genie definitions
├── src/
│   ├── common/                # shared utilities and audit logic
│   └── transforms/            # domain-specific transformation logic
└── databricks.yml             # Databricks Asset Bundle (DAB) manifest
```

## Implementation Logic
### Orchestrator
The `silver_orchestrator.py` acts as the single entry point for the Silver-layer tables. This design ensures that logging, error handling, and data-writing protocols remain identical across the project, reducing technical debt and ensuring that every table meets the same quality standards.

### Declarative Configuration (YAML)
Pipelines are defined using YAML files to separate the intent from the implementation.
* **Decoupling**: Changes to data sources or target paths do not require modifications to the underlying Python code.
* **Auditability**: The configuration provides a clear, human-readable map of data lineage.

### Incremental State Management (Watermarks)
The use of watermarks allows for efficient data processing:
* **Incremental Mode**: By defining a watermark_column, the orchestrator filters for new records only. This reduces compute costs and execution time.
* **Full Refresh**: In the absence of a watermark, the system performs a full rebuild. This is utilised for reference datasets or when historical corrections are required.

### Monitoring and AI/BI Integration
Operational health is managed through the `consumption/monitoring/` layer.
* **AI/BI Dashboards**: Real-time visibility into pipeline runs
* **Genie Capabilities**: The Silver tables are configured with rich metadata to support natural language querying (Genie). This allows stakeholders to ask questions in natural language without manual SQL intervention.


## Deployment
This project is deployed as a Databricks Asset Bundle (DAB).

**Environment Constraints**:
The current architecture is developed for the Databricks Free Edition (September 2026). In this version:
* Standard job triggers are used in place of advanced Delta Live Tables (DLT) features.
* Basic Unity Catalog functionality is utilised for metadata management.
* The transition path to the Paid/Enterprise tier is documented in `silver/docs/roadmap.md`.
