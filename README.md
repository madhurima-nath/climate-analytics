# Climate Analytics

## Overview

This repository contains a modular climate data pipeline built on Databricks Free Edition. It ingests raw climate, energy, and environmental data from multiple public sources, standardises it, and transforms it into analysis-ready fact tables stored in the `climate_energy_demand` Unity Catalog. The pipeline covers 250+ global territories, driven by a reference locations file in a Unity Catalog Volume.

The project targets **Physical Climate Risk (PCR)** analysis: combining weather observations, climate projections, energy demand, and land-use indicators into tables ready for AI/BI Dashboards and Genie natural language queries.

## Data Sources

The pipeline draws on four public data providers across five ingestion streams, each covering a distinct facet of physical climate risk:

| Source | What it provides | Why it's here |
| --- | --- | --- |
| **Open-Meteo** (Historical + Incremental) | Hourly weather observations (temperature, precipitation, wind, humidity) for any lat/lon | Foundation for current-condition analysis and the join key for energy demand modelling |
| **NOAA GSOD** | Daily weather station observations with multi-decade history | Provides station-level ground-truth weather with long temporal coverage for trend analysis |
| **OWID Energy** | Country-level energy consumption, production, and emissions metrics | Links climate variables to human energy demand and decarbonisation tracking |
| **FAOSTAT** | Land cover, forest inventory, and temperature change indicators by country | Connects climate trends to land-use and ecological resilience outcomes |
| **Open-Meteo CMIP6** | Climate model projections under SSP scenarios | Enables forward-looking risk analysis beyond historical observations |

Together these sources span weather observations, climate projections, energy demand, and land/forest indicators: the four pillars of a Physical Climate Risk analytics layer.

## Gold Layer and Analytical Use Cases

The Gold layer distills standardised Silver data into analysis-ready fact and dimension tables. Each fact table is designed to drive a specific class of analytical question:

| Gold table | Analytical use case |
| --- | --- |
| `fct_energy_demand` | Joins energy consumption with weather observations to analyse how temperature extremes drive heating and cooling demand; identify energy-vulnerable periods and regions |
| `fct_temp_change_annual` | Annual temperature-change indicators by country; supports multi-decade warming trend visualisation and anomaly tracking against historical baselines |
| `fct_land_cover_annual` | Year-over-year land cover transitions; enables deforestation detection, urbanisation tracking, and land-use change analysis |
| `fct_forest_resilience` | Combines forest inventory with temperature signals; scores forest ecosystem resilience under climate stress to identify vulnerable areas |
| `fct_ground_truth_audit` | Audit fact table; provides data-quality observability for pipeline monitoring dashboards |
| `dim_stations`, `dim_locations`, `dim_date`, `dim_koppen_zones` | Shared dimensions enabling cross-fact-table drill-downs by station, geography, time period, and Köppen climate zone (the standard climate classification system, used here to group regions by climate type for comparative analysis) |

## Design Decisions for Free Edition

This project is built for **Databricks Free Edition**, which constrains several architectural choices:

* **Full-reload Gold pipeline**: Free Edition does not support Delta Live Tables or incremental MERGE pipelines. Gold tables are dropped and recreated on each run via the orchestrator. This keeps the pipeline simple and idempotent at the cost of reprocessing the full Silver dataset each cycle.
* **YAML-driven transforms**: Each Silver and Gold table is declared in a YAML config that specifies schema, source query, column comments, and transformation function. The orchestrators read these configs at runtime, keeping the pipeline declarative and avoiding hard-coded SQL.
* **Notebook-based orchestration**: Each layer has a single orchestrator notebook (`silver_orchestrator`, `gold_orchestrator`) that iterates over its YAML configs and calls Python transform functions from `src/transforms/`. This replaces DLT-style declarative pipelines that are not available on Free Edition.
* **Serverless compute**: Jobs auto-attach serverless compute. No cluster configuration is needed, but wall-clock time and concurrency limits apply.
* **No streaming**: All ingestion is batch. Open-Meteo incremental notebooks fetch the latest available window on each run rather than maintaining a continuous stream.

## Pipeline Architecture

### Data Flow

Data flows through three layers, with each layer building on the previous one:

1. **Bronze (Ingestion)**: Raw data lands in Delta tables in `climate_energy_demand.bronze`. Open-Meteo notebooks pull weather data via API, driven by `reference_locations.csv` in the `/Volumes/climate_energy_demand/bronze/raw_uploads/` Unity Catalog Volume. OWID, NOAA, and FAOSTAT data is manually uploaded to the same Volume and then read into Delta tables. No cleaning or unit conversion happens here; raw data is preserved as-is with `ingested_at` and `source_file` metadata for traceability.
2. **Silver (Standardisation)**: The `silver_orchestrator` notebook reads YAML configs from `pipelines/silver/configs/`, queries the corresponding Bronze tables, and calls Python transform functions from `src/transforms/` to normalise units, align temporal grains, clean cross-source inconsistencies, and build shared dimensions. Results land in `climate_energy_demand.silver`.
3. **Gold (Analytics)**: The `gold_orchestrator` notebook reads YAML configs from `pipelines/gold/configs/`, queries Silver tables, and calls transform functions to build denormalised fact and dimension tables in `climate_energy_demand.gold`. Gold tables are fully rebuilt on each run.

### Modular Build

Each layer can run independently. This is a deliberate design choice:

* **Bronze**: Each source has its own ingestion notebook (`01_ingest_openmeteo_historical`, `01_ingest_openmeteo_incremental`, `02_ingest_owid_energy`, `03_ingest_noaa_gsod`, `04_ingest_openmeteo_cmip6_projections`, `05_ingest_fao_data`). Trigger any one to refresh a single source.
* **Silver**: A single orchestrator processes all Silver tables. Run it when Bronze data has changed.
* **Gold**: A single orchestrator rebuilds all Gold tables from Silver. Run it when Silver transforms have been updated.
* **Validation**: Each layer has a dedicated pytest validation job that runs on its own.

This modular design enables selective reprocessing (only re-run the layer that changed), independent testing during development, and flexible CI/CD. You can trigger the full end-to-end pipeline or just the slice that needs refreshing.

Each layer has its own README with table details:

* [Bronze README](pipelines/bronze/README.md)
* [Silver README](pipelines/silver/README.md)
* [Gold README](pipelines/gold/README.md)

### Monitoring and Audit

A custom audit framework tracks every pipeline run. Execution metadata (run IDs, table names, row counts, durations) is persisted to Delta tables in `climate_energy_demand.monitoring`: `pipeline_runs` for job execution tracking and `test_results` for individual test outcomes. Bootstrap verification is handled by `setup/verify_bootstrap.sql`. See `pipelines/consumption/monitoring/design_doc.md` for the full schema and dashboard build guide.

## Getting Started

### Prerequisites

* Databricks Free Edition workspace
* Databricks CLI installed locally, or use the Databricks UI to deploy the bundle

### Quick Start

1. **Deploy the bundle**: Run `databricks bundle deploy` from the CLI, or deploy via the Databricks UI. This creates the jobs defined in `databricks.yml`.

2. **Bootstrap infrastructure**: Run the `project_bootstrap` job once. This creates the `climate_energy_demand` catalog, schemas (bronze, silver, gold, monitoring), and audit and monitoring tables. It is idempotent (`IF NOT EXISTS`).

3. **Ingest Bronze data**: Run the Bronze ingestion notebooks. Open-Meteo notebooks pull data via API automatically. For OWID, NOAA, and FAOSTAT, download the source files and upload them to `/Volumes/climate_energy_demand/bronze/raw_uploads/`, then run the corresponding ingestion notebook.

4. **Run the pipeline**: Choose one:
   * **Full end-to-end**: Trigger `climate_data_pipeline` (chains: bronze validate, silver load, silver validate, gold load, gold validate).
   * **Layer-by-layer**: Run individual jobs as needed. For example, `silver_data_load` then `silver_validation` when Bronze data has changed, or `gold_data_load` then `gold_validation` when Silver transforms have been updated.

5. **Explore the results**: Open the AI/BI Dashboard or query the Gold tables directly via SQL.

### Pipeline Jobs

| Job | What it does | When to use |
| --- | --- | --- |
| `climate_data_pipeline` | Chains: bronze validate, silver load, silver validate, gold load, gold validate | Full end-to-end run, CI/CD |
| `bronze_validation` | Runs bronze layer data quality tests | After new bronze data uploads |
| `silver_data_load` + `silver_validation` | Silver orchestrator only, then validation | When bronze data changed |
| `gold_data_load` + `gold_validation` | Gold orchestrator only, then validation | When re-running gold transforms |

The modular jobs are decoupled; trigger each manually. The full pipeline chains everything with task dependencies so a single trigger runs all steps in sequence. Bronze validation uses `run_if: ALL_DONE` so stale data is flagged without blocking the pipeline.

## Intelligence Layer: Current State and Next Steps

The pipeline is designed to feed into Databricks AI/BI and Genie, but this layer is not yet fully built out.

**What is built:**
* Every Silver and Gold table has column-level comments declared in YAML configs. The orchestrators register these comments to Unity Catalog automatically, giving Genie the semantic context it needs to interpret the tables.
* An AI/BI Dashboard definition (`.lvdash.json`) exists in the `dashboard/` directory and is deployed as a bundle resource. It covers pipeline monitoring (run status, test results, row counts).
* Monitoring tables (`pipeline_runs`, `test_results`) are populated on every job run.

**What can be done next:**
* Build a climate insights dashboard on top of the Gold fact tables (warming trends, energy demand patterns, forest resilience scores, land cover change).
* Set up a Genie space pointed at the Gold tables. The column comments are already in place, so Genie can answer natural language questions like "show the five-year warming trend for coastal regions" once a space is created.
* Add curated Genie instructions or example queries to improve answer quality for domain-specific questions.

## Repository Structure

```
climate-analytics
├── databricks.yml                    # Declarative Automation Bundle manifest
├── setup/                            # Infrastructure bootstrap SQL scripts
├── pipelines/
│   ├── bronze/                       # Ingestion notebooks (source-specific)
│   │   └── notebooks/
│   ├── silver/                       # Standardisation and cleaning
│   │   ├── configs/                   # YAML declarative table definitions
│   │   ├── docs/                     # Design, architectural decisions, roadmap
│   │   ├── silver_orchestrator       # Centralised execution engine (notebook)
│   │   └── setup_silver.sql           # Creates silver.ingestion_audit table
│   ├── gold/                         # Aggregated analytics fact tables
│   │   ├── configs/                   # YAML table definitions (facts + dimensions)
│   │   ├── docs/                     # Design doc and architecture logic
│   │   ├── source_to_target_mappings/  # Lineage CSVs (Silver to Gold)
│   │   ├── gold_orchestrator         # Config-driven transformation runner (notebook)
│   │   └── setup_gold.sql            # Creates gold.ingestion_audit table
│   └── consumption/
│       └── monitoring/               # Monitoring table setup and dashboard design doc
├── src/
│   ├── transforms/                   # Domain-specific transformation modules
│   ├── common/                       # Shared utilities (audit, shared logic)
│   └── tests/                        # pytest suites (bronze, silver, gold, infrastructure)
├── dashboard/                        # AI/BI Dashboard (.lvdash.json)
├── requirements.txt                  # Python dependencies
└── README.md
```

- `setup/`: Two SQL scripts. `project_infrastructure.sql` creates the `climate_energy_demand` catalog, all schemas (bronze, silver, gold, monitoring), and monitoring tables. `verify_bootstrap.sql` checks that the infrastructure exists before pipeline jobs are run.
- `pipelines/bronze/`: Source-specific ingestion notebooks. Data is driven by `reference_locations.csv` stored in the `/Volumes/climate_energy_demand/bronze/raw_uploads/` Unity Catalog Volume. See [Bronze README](pipelines/bronze/README.md) for the full source catalogue.
- `pipelines/silver/`: YAML configs, the orchestrator notebook, and `setup_silver.sql` (creates the `silver.ingestion_audit` table for watermark tracking). See [Silver README](pipelines/silver/README.md) for table details.
- `pipelines/gold/`: YAML configs, the orchestrator notebook, source-to-target mapping CSVs, and `setup_gold.sql` (creates the `gold.ingestion_audit` table for watermark tracking). See [Gold README](pipelines/gold/README.md) for table details.
- `pipelines/consumption/monitoring/`: Monitoring table setup and the dashboard design doc.
- `src/transforms/`: Six Python modules: `energy.py` (energy demand transforms), `weather.py` (weather observation and projection transforms), `climate.py` (temperature change transforms), `nature.py` (land cover and forest resilience transforms), `quality.py` (data quality checks), and `common.py` (shared transform helpers).
- `src/common/`: `audit_utils.py` (watermark tracking and audit-table writes) and `shared_logic.py` (common transformation helpers reused across domains).
- `src/tests/`: `test_infrastructure.py` (bootstrap verification), `test_bronze_tables.py`, `test_silver_tables.py`, `test_gold_tables.py`, `test_shared_logic.py`, and `test_audit_utils.py`. A `run_all_tests_notebook` notebook is provided for running tests inside Databricks.
- `dashboard/`: AI/BI Dashboard definition deployed as a bundle resource.
- `requirements.txt`: Python libraries required for the project.
