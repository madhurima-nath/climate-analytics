# Climate Analytics

## Overview

This repository contains a modular climate data pipeline built on Databricks Free Edition. It ingests raw climate, energy, and environmental data from multiple public sources, standardises it, and transforms it into analysis-ready fact tables stored in the `climate_energy_demand` Unity Catalog. The pipeline covers 250+ global territories, driven by a reference locations file in a Unity Catalog Volume.

The project targets **Physical Climate Risk (PCR)** analysis: combining weather observations, climate projections, energy demand, and land-use indicators into tables ready for AI/BI Dashboards and Genie natural language queries.

New to the project? Start with [How the Pipeline Works](#how-the-pipeline-works), then use the [Documentation Map](#documentation-map) to go deeper.

## How the Pipeline Works

### The three layers

Data moves through three layers in the `climate_energy_demand` catalog, each with one responsibility and its own validation job:

1. **Bronze, raw mirror.** Every source lands in its own Delta table exactly as delivered. Nothing is cleaned or converted here, deliberately: Fahrenheit readings, non-standard headers, and source-specific missing-value flags are all preserved, and every row is tagged with `ingested_at` and `source_file`. Because the raw data is untouched, any later correction can be replayed from the originals. Open-Meteo data is pulled via API, driven by `reference_locations.csv` (250+ territories); OWID, NOAA, and FAOSTAT files are uploaded manually to the `raw_uploads` Unity Catalog Volume and then ingested into governed Delta tables, because Free Edition network restrictions prevent direct downloads. Source-by-source design: [Bronze README](pipelines/bronze/README.md).

2. **Silver, the standardisation gate.** The `silver_orchestrator` notebook processes one YAML config per table (`pipelines/silver/configs/*.yml`). For each config it reads the last watermark from `silver.ingestion_audit`, extracts only new Bronze rows past that watermark, calls the table's transform function in `src/transforms/` (units to metric, wide FAO/OWID tables pivoted to one row per year per metric, shared dimensions built once for every pipeline), MERGEs the result into the target on the config's merge keys, and writes the new watermark back to the audit table. Each config runs inside its own error handling, so one failing table is logged and skipped while the rest complete. Adding a Silver table means adding a YAML file: the orchestrator discovers it automatically. Deep detail: [Silver README](pipelines/silver/README.md), [silver design doc](pipelines/silver/docs/design_doc.md), and the [architecture decision records](pipelines/silver/docs/architectural_decisions.md).

3. **Gold, rebuilt analytics.** The `gold_orchestrator` reads `pipelines/gold/configs/*.yml` and rebuilds every fact and dimension table from Silver on each run. Rebuilding rather than upserting is deliberate: Gold tables are derived aggregates with no independent truth of their own, so Silver remains the single source of truth and Gold can never drift from it. Deep detail: [Gold README](pipelines/gold/README.md), [gold design doc](pipelines/gold/docs/design_doc.md), and [architecture logic](pipelines/gold/docs/architecture_logic.md).

Why layers at all: each layer depends only on the one before it, any layer can be rerun independently, and validation jobs run between layers so problems surface where they were caused.

### Two ways to run Silver and Gold

Silver and Gold can be run through either of two orchestration approaches, deployed side by side and writing to separate schemas so they never conflict:

* **Notebook orchestration** (the production baseline today): config-driven orchestrator jobs as described above, with manual watermarks in `ingestion_audit`, per-layer scheduling, failure isolation per table, and monitoring through the `pipeline_runs` table.
* **Lakeflow Spark Declarative Pipelines (SDP)** (the strategic direction): the same tables declared as `@dlt.table` functions in a single pipeline, `silver_gold_sdp`, where streaming reads create the Silver streaming tables and batch reads create the Gold materialized views, writing to the `silver_sdp` and `gold_sdp` schemas. Automatic checkpointing replaces the manual watermarks, and the built-in SDP event log replaces the audit tables. The lane's first full update completed in October 2026 and row-count parity with the notebook lane is validated.

Both approaches call the same `src/transforms/*.py` functions, so a bug fix in a transform benefits both at once. This is a staged migration with explicit exit criteria, not an open-ended choice: SDP is the intended end state for Silver and Gold, while Bronze stays notebook-based in both because its ingestion is REST-API driven. The full comparison, coexistence rules, exit criteria, and when to use which: [Pipeline Approaches](pipeline_approaches.md). The decision record: [ADR #7](pipelines/silver/docs/architectural_decisions.md#7-dual-pipeline-approaches-notebook-orchestration-and-sdp-coexistence).

### How the jobs are wired

Everything above is deployed from `databricks.yml`, the Declarative Automation Bundle manifest:

* `project_bootstrap` runs once and creates the catalog, all schemas (including `silver_sdp` and `gold_sdp`), and the audit and monitoring tables. It is idempotent and verified by `setup/verify_bootstrap.sql`.
* Each layer has independent load and validation jobs (`silver_data_load`, `silver_validation`, `gold_data_load`, `gold_validation`, `bronze_validation`) that can be triggered individually.
* `climate_data_pipeline` chains the full sequence: bronze validate, silver load, silver validate, gold load, gold validate. Bronze validation uses `run_if: ALL_DONE`, so stale Bronze data is flagged without blocking the rest of the run.
* The SDP pipeline `silver_gold_sdp` is created by the same deploy and started manually from the Pipelines page.

The job table with when-to-use guidance is in [Getting Started](#getting-started).

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

## What the Gold Layer Answers

The Gold layer distils standardised Silver data into analysis-ready fact and dimension tables. Each fact table is designed to drive a specific class of analytical question:

| Gold table | Analytical use case |
| --- | --- |
| `fct_energy_demand_daily` | Joins energy consumption with weather observations to analyse how temperature extremes drive heating and cooling demand; identify energy-vulnerable periods and regions |
| `fct_temp_change_annual` | Annual temperature-change indicators by country; supports multi-decade warming trend visualisation and anomaly tracking against historical baselines |
| `fct_land_cover_annual` | Year-over-year land cover transitions; enables deforestation detection, urbanisation tracking, and land-use change analysis |
| `fct_forest_resilience_annual` | Combines forest inventory with temperature signals; scores forest ecosystem resilience under climate stress to identify vulnerable areas |
| `fct_ground_truth_verification_daily` | Compares modelled Open-Meteo weather against NOAA station observations to quantify model reliability per country and station |
| `dim_stations`, `dim_locations`, `dim_date`, `dim_koppen_zones` | Shared dimensions enabling cross-fact-table drill-downs by station, geography, time period, and Köppen climate zone (the standard climate classification system, used here to group regions by climate type for comparative analysis) |

## Why the Architecture Is This Way

### Free Edition constraints are design inputs

The platform runs on Databricks Free Edition, and its limits shaped the choices rather than being worked around:

* **Serverless only**: no cluster configuration, but wall-clock time and concurrency limits apply, so nothing assumes long-running compute.
* **Batch ingestion**: all Bronze ingestion is batch; the incremental Open-Meteo notebooks fetch the latest window on each run rather than maintaining a stream.
* **Concurrency**: at most 5 concurrent job tasks and 1 active SDP pipeline per pipeline type, which is why the SDP Silver and Gold tables share a single pipeline.
* **Egress restrictions**: several sources cannot be fetched directly, so they are uploaded manually to a UC Volume and ingested from there (patterns per source in the [Bronze README](pipelines/bronze/README.md)).

### Every table is a YAML config

Each Silver and Gold table is declared in a YAML file that names its sources, transform function, merge keys, watermark column, and column comments. This keeps the intent of each table (the config) separate from its implementation (`src/transforms/`), makes adding a table a config-file change rather than a code change, and gives AI/BI Genie its semantic context: the column comments declared in the configs are registered to Unity Catalog automatically. Because the configs are version-controlled, the data lineage map is reviewable in Git alongside the code.

### The bundle manifest is the project inventory

`databricks.yml` declares every job, the SDP pipeline, and the dashboard in one place. A single command (`databricks bundle deploy`) reproduces the entire setup in a workspace, changes are reviewed in Git before they reach the workspace, and the manifest is the one place to consult to see what exists.

### Incremental where it pays, rebuild where it is simpler

Silver loads incrementally using watermarks stored in queryable Delta tables rather than opaque checkpoint files: data freshness can be inspected with SQL, state survives restarts, and the compute budget is spent only on new data. Gold rebuilds fully on each run because rebuilding a derived aggregate is simpler and safer than upserting it, and eliminates any possibility of drift from Silver.

For the decision records behind these choices, see the [architecture decision records](pipelines/silver/docs/architectural_decisions.md). For how these choices change on a paid tier, see the [roadmap](pipelines/silver/docs/roadmap.md).

## Testing and Validation

Every layer has a pytest suite, run as a Databricks job through `src/tests/run_all_tests_notebook`:

* **Infrastructure**: catalog, schemas, and audit tables exist.
* **Bronze**: tables exist, are populated, and weather data is fresh (7-day threshold).
* **Silver**: reasonable row counts, no null primary keys, temperatures genuinely in Celsius, no duplicate primary keys, the wide-to-long normalisation worked, and watermarking is functioning.
* **Gold**: fact tables exist, business logic holds (DSI ≥ 0, carbon density correctness), primary keys are unique, referential integrity back to Silver holds, and model-vs-observation accuracy is checked per country (Greece's known station-grain bias is documented as a warning; see [ADR #8](pipelines/silver/docs/architectural_decisions.md#8-modelled-vs-observed-grain-mismatch-known-limitation)).

Validation runs are smart: they compare the last data load against the last validation run and skip when nothing has changed, which saves compute on Free Edition. The suites have caught real problems that job status alone never showed: null primary keys, a physically impossible 5,537°C maximum temperature, and duplicate rows introduced by MERGEs. The full guide, including how to run the tests and customise thresholds: [src/tests README](src/tests/README.md).

Every orchestrator and validation run also writes to `climate_energy_demand.monitoring`: `pipeline_runs` (per-task and per-table outcomes) and `test_results` (one row per test function with parsed, readable errors). The AI/BI monitoring dashboard reads these tables. Full schema and dashboard queries: [monitoring design doc](pipelines/consumption/monitoring/design_doc.md).

## Getting Started

### Prerequisites

* Databricks Free Edition workspace
* Databricks CLI installed locally, or use the Databricks UI to deploy the bundle

### Quick Start

1. **Deploy the bundle**: Run `databricks bundle deploy` from the CLI, or deploy via the Databricks UI. This creates the jobs defined in `databricks.yml`.

2. **Bootstrap infrastructure**: Run the `project_bootstrap` job once. This creates the `climate_energy_demand` catalog, schemas (bronze, silver, gold, silver_sdp, gold_sdp, monitoring), and audit and monitoring tables. It is idempotent (`IF NOT EXISTS`).

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

The modular jobs are decoupled; trigger each manually. The full pipeline chains everything with task dependencies so a single trigger runs all steps in sequence. Bronze validation uses `run_if: ALL_DONE` so stale data is flagged without blocking the pipeline. The SDP pipeline (`silver_gold_sdp`) is a separate bundle resource: deploy creates it, and you start it manually from the Pipelines page.

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
│   │   ├── silver_sdp.py             # SDP source file (declarative silver tables)
│   │   └── setup_silver.sql           # Creates silver.ingestion_audit table
│   ├── gold/                         # Aggregated analytics fact tables
│   │   ├── configs/                   # YAML table definitions (facts + dimensions)
│   │   ├── docs/                     # Design doc and architecture logic
│   │   ├── source_to_target_mappings/  # Lineage CSVs (Silver to Gold)
│   │   ├── gold_orchestrator         # Config-driven transformation runner (notebook)
│   │   ├── gold_sdp.py               # SDP source file (declarative gold materialized views)
│   │   └── setup_gold.sql            # Creates gold.ingestion_audit table
│   └── consumption/
│       └── monitoring/               # Monitoring table setup and dashboard design doc
├── src/
│   ├── transforms/                   # Domain-specific transformation modules
│   ├── common/                       # Shared utilities (audit, shared logic)
│   └── tests/                        # pytest suites (bronze, silver, gold, infrastructure)
├── pipeline_approaches.md            # Notebook vs SDP comparison, coexistence rules, exit criteria
├── dashboard/                        # AI/BI Dashboard (.lvdash.json)
├── requirements.txt                  # Python dependencies
└── README.md
```

- `setup/`: Two SQL scripts. `project_infrastructure.sql` creates the `climate_energy_demand` catalog and all schemas (bronze, silver, gold, silver_sdp, gold_sdp, monitoring). `verify_bootstrap.sql` checks that the infrastructure exists before pipeline jobs are run.
- `pipelines/bronze/`: Source-specific ingestion notebooks. Data is driven by `reference_locations.csv` stored in the `/Volumes/climate_energy_demand/bronze/raw_uploads/` Unity Catalog Volume. See [Bronze README](pipelines/bronze/README.md) for the full source catalogue.
- `pipelines/silver/`: YAML configs, the orchestrator notebook, and `setup_silver.sql` (creates the `silver.ingestion_audit` table for watermark tracking). See [Silver README](pipelines/silver/README.md) for table details.
- `pipelines/gold/`: YAML configs, the orchestrator notebook, source-to-target mapping CSVs, and `setup_gold.sql` (creates the `gold.ingestion_audit` table for watermark tracking). See [Gold README](pipelines/gold/README.md) for table details.
- `pipelines/consumption/monitoring/`: Monitoring table setup and the dashboard design doc.
- `src/transforms/`: Six Python modules: `energy.py` (energy demand transforms), `weather.py` (weather observation and projection transforms), `climate.py` (temperature change transforms), `nature.py` (land cover and forest resilience transforms), `quality.py` (data quality checks), and `common.py` (shared transform helpers).
- `src/common/`: `audit_utils.py` (watermark tracking and audit-table writes) and `shared_logic.py` (common transformation helpers reused across domains).
- `src/tests/`: `test_infrastructure.py` (bootstrap verification), `test_bronze_tables.py`, `test_silver_tables.py`, `test_gold_tables.py`, `test_shared_logic.py`, and `test_audit_utils.py`. A `run_all_tests_notebook` notebook is provided for running tests inside Databricks.
- `dashboard/`: AI/BI Dashboard definition deployed as a bundle resource.
- `requirements.txt`: Python libraries required for the project.

## Documentation Map

Every document in the repository, one line each:

| Document | What it covers |
| --- | --- |
| [Pipeline Approaches](pipeline_approaches.md) | Notebook orchestration vs SDP: comparison, coexistence rules, exit criteria, when to use which |
| [Bronze README](pipelines/bronze/README.md) | Per-source ingestion design: patterns, resilience, Free Edition workarounds |
| [Silver README](pipelines/silver/README.md) | Silver layer architecture and table catalogue |
| [Silver design doc](pipelines/silver/docs/design_doc.md) | Orchestration flow, degree-day logic, data quality fixes |
| [Architecture decision records](pipelines/silver/docs/architectural_decisions.md) | ADRs 1-11 and the config file walkthroughs |
| [Roadmap](pipelines/silver/docs/roadmap.md) | Free Edition to paid tier evolution path |
| [Gold README](pipelines/gold/README.md) | Gold layer architecture and the table registry |
| [Gold design doc](pipelines/gold/docs/design_doc.md) | Per-fact-table engineering logic and metrics |
| [Gold architecture logic](pipelines/gold/docs/architecture_logic.md) | Data flow patterns, performance optimisations, Genie integration |
| [Source-to-target mappings](pipelines/gold/source_to_target_mappings/gold_source_to_target_mappings.md) | Column-level lineage from Silver to Gold |
| [Monitoring design doc](pipelines/consumption/monitoring/design_doc.md) | Monitoring table schema, design decisions, dashboard queries |
| [Tests README](src/tests/README.md) | Test suite guide: what is validated, how to run, how to customise |
| Genie space guide (TODO: link) | To be added |
| Climate domain notes (TODO: link) | To be added |
