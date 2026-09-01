# Climate Analytics

## Overview
This repository contains a modular, end-to-end climate data pipeline built on Databricks. It is designed to ingest multi-source raw climate data and transform it into high-fidelity, queryable assets for natural language inquiry and automated reporting.

## Data Lifecycle: Medallion Architecture
The data flows through three distinct layers, ensuring data integrity from ingestion to insight:
1. **Bronze (Ingestion)**: Raw data ingestion from multiple climate sources (Notebooks).
2. **Silver (Standardisation)**:  Focused on unit normalisation, temporal alignment, and cross-source cleaning.
3. **Gold (Analytics)**: Aggregated datasets optimised and curated for AI/BI Dashboards and Genie AI inquiries.

## Infrastructure as Code (IaC)
To ensure reproducibility, the project is structured for *Databricks Asset Bundles (DABs)*:
- **Deployment**: Resource mappings and environment settings are centrally defined in `databricks.yml`.
- **Modularity**: A `src/` directory serves as a placeholder for refactoring shared logic (e.g. mathematical models or schemas) into `.py` modules as the system matures.

## Operational Framework
Given the functional limitations of the *Databricks Free Tier*, the follwing steps are implemented to maintain production-grade standards.

1. *Pipeline Orchestration*:  In the absence of Serverless Workflows (Jobs) or Delta Live Tables (DLT), a Master Orchestrator Notebook is utilised. This script programmatically manages the dependency graph and triggers pipeline stages in the correct sequence, simulating a standard Directed Acyclic Graph (DAG).

2. *Execution Monitoring & Audit*: Since enterprise System Tables are unavailable, a custom Audit Framework has been developed:
- Pipeline Logging: Execution metadata—including notebook identity, run duration, and row counts—is persisted to a telemetry_audit Delta table.
- Validation: Placeholder cells for schema enforcement and anomaly detection are integrated to maintain data provenance.

## Intelligence Layer (AI/BI & Genie)
The final delivery layer leverages Databricks AI/BI Dashboards and the Genie semantic agent.
- *Semantic Context*: The Gold layer includes version-controlled "Instructions" and metadata, enabling stakeholders to perform natural language inquiries (e.g. "Identify the five-year warming trend for coastal regions").
- *Asset Management*: Dashboard definitions and semantic aliases are stored as code within the reporting/ directory.


## Repository Structure
```
climate-analytics
├── databricks.yml          # IaC Bundle Manifest
├── pipelines/              
│   ├── 01_bronze/          # Ingestion Notebooks (Source-Specific)
│   ├── 02_silver/          # Standardisation & Cleaning (Drafts)
│   └── 03_gold/            # Aggregated Analytics Tables (Drafts)
├── orchestration/          # Master Script for pipeline execution
├── src/                    # [Placeholder] For shared .py logic and schemas
├── reporting/              # AI/BI Dashboard exports & Genie context
├── requirements.txt        # Python dependencies (e.g. databricks-cli)
└── README.md
```

- `pipelines/`: Contains the core transformation logic separated by Medallion tier.
- `src/`: [Placeholder] Reserved for refactoring shared logic into Python modules (.py) as the project matures.
- `reporting/`: Configuration files for AI/BI Dashboards and Genie natural language instructions.
- `requirements.txt`: List of Python libraries required for the Databricks cluster.
