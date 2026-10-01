# Climate Analytics

## Overview
This repository contains a modular, end-to-end climate data pipeline built on Databricks. It ingests multi-source raw climate, energy, and environmental data and transforms it into high-fidelity, queryable assets for AI/BI Dashboards and Genie natural language inquiry.

## Why These Datasets?

The pipeline draws on four public data sources, each chosen to cover a distinct facet of physical climate risk:

| Source | What it provides | Why it's here |
| --- | --- | --- |
| **Open-Meteo** (Historical + Incremental) | Hourly weather observations (temperature, precipitation, wind, humidity) for any lat/lon | Foundation for current-condition analysis and the join key for energy demand modelling |
| **NOAA GSOD** | Daily weather station observations with multi-decade history | Provides station-level ground-truth weather with long temporal coverage for trend analysis |
| **OWID Energy** | Country-level energy consumption, production, and emissions metrics | Links climate variables to human energy demand and decarbonisation tracking |
| **FAOSTAT** | Land cover, forest inventory, and temperature change indicators by country | Connects climate trends to land-use and ecological resilience outcomes |
| **Open-Meteo CMIP6** | Climate model projections under SSP scenarios | Enables forward-looking risk analysis beyond historical observations |

Together these sources span weather observations, climate projections, energy demand, and land/forest indicators — the four pillars needed for a Physical Climate Risk (PCR) analytics layer on Databricks Free Edition.

## Why These Gold Tables?

The Gold layer distills the standardised Silver data into analysis-ready fact and dimension tables:

| Gold table | Purpose |
| --- | --- |
| `fct_energy_demand` | Joins energy consumption with weather observations to expose the relationship between climate and energy use |
| `fct_temp_change_annual` | Annual temperature-change indicators by country, ready for trend visualisation |
| `fct_land_cover_annual` | Year-over-year land cover transitions to detect deforestation and degradation patterns |
| `fct_forest_resilience` | Combines forest inventory with temperature signals to score forest ecosystem resilience |
| `fct_ground_truth_audit` | Audit fact table for data-quality monitoring and pipeline health dashboards |
| `dim_stations`, `dim_locations`, `dim_date`, `dim_koppen_zones` | Shared dimensions enabling cross-fact-table drill-downs by station, geography, time, and climate zone |

## Design Decisions for Free Edition

This project is built for **Databricks Free Edition**, which constrains several architectural choices:

* **Full-reload Gold pipeline**: Free Edition does not support Delta Live Tables or incremental MERGE pipelines. Gold tables are dropped and recreated on each run via the orchestrator. This keeps the pipeline simple and idempotent at the cost of reprocessing the full Silver dataset each cycle.
* **YAML-driven transforms**: Each Silver and Gold table is declared in a YAML config that specifies schema, source query, column comments, and transformation function. The orchestrators read these configs at runtime, keeping the pipeline declarative and avoiding hard-coded SQL.
* **Notebook-based orchestration**: Each layer has a single orchestrator notebook (`silver_orchestrator`, `gold_orchestrator`) that iterates over its YAML configs and calls Python transform functions from `src/transforms/`. This replaces DLT-style declarative pipelines that aren't available on Free Edition.
* **Serverless compute**: Jobs auto-attach serverless compute — no cluster configuration is needed, but wall-clock time and concurrency limits apply.
* **No streaming**: All ingestion is batch. Open-Meteo incremental notebooks fetch the latest available window on each run rather than maintaining a continuous stream.

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

- `setup/`: Two SQL scripts — `project_infrastructure.sql` creates the `climate_energy_demand` catalog, all schemas (bronze, silver, gold, monitoring), and monitoring tables; `verify_bootstrap.sql` validates that the infrastructure exists before pipeline jobs are run.
- `pipelines/`: Core transformation logic separated by Medallion tier, plus the consumption/monitoring layer.
- `src/transforms/`: Six Python modules implementing domain-specific transformation logic — `energy.py` (energy demand transforms), `weather.py` (weather observation/projection transforms), `climate.py` (temperature change transforms), `nature.py` (land cover and forest resilience transforms), `quality.py` (data quality checks), and `common.py` (shared transform helpers).
- `src/common/`: Shared utilities — `audit_utils.py` (watermark tracking and audit-table writes) and `shared_logic.py` (common transformation helpers reused across domains).
- `src/tests/`: Automated pytest suites — `test_infrastructure.py` (bootstrap verification), `test_bronze_tables.py`, `test_silver_tables.py`, `test_gold_tables.py`, `test_shared_logic.py`, and `test_audit_utils.py`. A `run_all_tests_notebook` notebook is provided for Databricks-native test execution.
- `dashboard/`: Version-controlled AI/BI Dashboard definition deployed as a bundle resource.
- `requirements.txt`: Python libraries required for the project.
