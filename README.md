# Climate Analytics

## Overview
This repository contains a modular, end-to-end climate data pipeline built on Databricks. It ingests multi-source raw climate, energy, and environmental data and transforms it into high-fidelity, queryable assets for AI/BI Dashboards and Genie natural language inquiry.

## Data Lifecycle: Medallion Architecture
The data flows through three distinct layers, ensuring data integrity from ingestion to insight:
1. **Bronze (Ingestion)**: Raw data ingestion from multiple climate sources (Open-Meteo, OWID, NOAA GSOD, FAOSTAT) via source-specific notebooks.
2. **Silver (Standardisation)**: Unit normalisation, temporal alignment, cross-source cleaning, and dimensional harmonisation driven by YAML configs and Python transform modules.
3. **Gold (Analytics)**: Denormalised, analysis-ready fact tables optimised for Physical Climate Risk (PCR) analysis, AI/BI Dashboards, and Genie natural language queries.

## Infrastructure as Code (IaC)
The project is deployed via Declarative Automation Bundles:
- **Deployment**: Resource mappings, jobs, dashboards, and environment settings are centrally defined in `databricks.yml`.
- **Modularity**: The `src/` directory contains shared Python modules for transforms (`src/transforms/`), utilities (`src/common/`), and tests (`src/tests/`).

## Operational Framework

1. **Pipeline Orchestration**: Jobs are defined in `databricks.yml` and deployed via Declarative Automation Bundles. Each layer has modular jobs (data load + validation) that run independently, plus a full end-to-end pipeline job that chains all layers with task dependencies.

2. **Execution Monitoring & Audit**: A custom audit framework tracks every pipeline run:
   - **Pipeline Logging**: Execution metadata, including run IDs, table names, row counts, and durations, is persisted to Delta tables in `climate_energy_demand.monitoring`.
   - **Validation**: Automated pytest suites validate table existence, data quality, unit conversions, primary key uniqueness, and business logic across all layers.
   - **Dashboard**: An AI/BI Dashboard provides real-time visibility into pipeline health, test results, and data load outcomes.

## Job Execution & Prerequisites

All jobs are defined in `databricks.yml` and deployed via Declarative Automation Bundles.

### Prerequisite: Infrastructure Bootstrap (one-time)

Run `project_bootstrap` **once** before any pipeline job. This creates the catalog, schemas, audit tables, and monitoring tables. It is idempotent (uses `IF NOT EXISTS`) but should not be re-run unless infrastructure changes are needed.

### Pipeline Jobs

Four options exist after bootstrap:

| Job | What it does | When to use |
| --- | --- | --- |
| `climate_data_pipeline` | Chains: bronze validate → silver load → silver validate → gold load → gold validate | Full end-to-end run, CI/CD |
| `bronze_validation` | Runs bronze layer data quality tests | After new bronze data uploads |
| `silver_data_load` + `silver_validation` | Silver orchestrator only, then validation | When bronze data changed |
| `gold_data_load` + `gold_validation` | Gold orchestrator only, then validation | When re-running gold transforms |

The modular jobs are decoupled — trigger each manually. The full pipeline chains everything with task dependencies so a single trigger runs all steps in sequence. Bronze validation uses `run_if: ALL_DONE` so stale data is flagged without blocking the pipeline.

### Monitoring

All job runs are tracked in `climate_energy_demand.monitoring` tables: `pipeline_runs` (job execution tracking with per-table detail) and `test_results` (individual test outcomes with clean error parsing). Bootstrap verification is handled by `setup/verify_bootstrap.sql`. See `pipelines/consumption/monitoring/design_doc.md` for the full schema and dashboard build guide.

## Intelligence Layer (AI/BI & Genie)
The final delivery layer leverages Databricks AI/BI Dashboards and the Genie semantic agent.
- **Semantic Context**: Every Gold and Silver table includes column-level comments auto-registered in Unity Catalog, enabling stakeholders to perform natural language inquiries (e.g. "Identify the five-year warming trend for coastal regions").
- **Asset Management**: The AI/BI Dashboard definition is version-controlled as a `.lvdash.json` file in the `dashboard/` directory and deployed as a bundle resource.
- **Genie Enablement**: Genie semantic understanding is driven by column-level `COMMENT` properties in the YAML configs, which are auto-registered to Unity Catalog by the orchestrators. No separate Genie instruction files are required.


## Repository Structure
```
climate-analytics
├── databricks.yml              # Declarative Automation Bundle manifest
├── setup/                       # Infrastructure bootstrap SQL scripts
├── pipelines/
│   ├── bronze/                  # Ingestion notebooks (source-specific)
│   │   └── notebooks/
│   ├── silver/                  # Standardisation & cleaning
│   │   ├── configs/              # YAML declarative table definitions
│   │   ├── docs/                 # Design, architectural decisions, roadmap
│   │   ├── silver_orchestrator   # Centralised execution engine (notebook)
│   │   └── setup_silver.sql
│   ├── gold/                    # Aggregated analytics fact tables
│   │   ├── configs/              # YAML table definitions (facts + dimensions)
│   │   ├── docs/                 # Design doc and architecture logic
│   │   ├── source_to_target_mappings/  # Lineage CSVs (Silver → Gold)
│   │   ├── gold_orchestrator     # Config-driven transformation runner (notebook)
│   │   └── setup_gold.sql
│   └── consumption/
│       └── monitoring/          # Monitoring table setup + dashboard design doc
├── src/
│   ├── transforms/              # Domain-specific transformation modules
│   ├── common/                   # Shared utilities (audit, shared logic)
│   └── tests/                    # pytest suites (bronze, silver, gold, infrastructure)
├── dashboard/                   # AI/BI Dashboard (.lvdash.json)
├── requirements.txt             # Python dependencies
└── README.md
```

- `pipelines/`: Core transformation logic separated by Medallion tier, plus the consumption/monitoring layer.
- `src/transforms/`: Python modules implementing the transformation logic for each domain (energy, weather, climate, nature, quality).
- `src/common/`: Shared utilities including audit watermarking and common transformation logic.
- `src/tests/`: Automated pytest suites validating infrastructure, bronze, silver, and gold layers.
- `dashboard/`: Version-controlled AI/BI Dashboard definition deployed as a bundle resource.
- `requirements.txt`: Python libraries required for the project.
