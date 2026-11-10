# Climate Analytics

## Overview

This repository contains a modular climate data platform with a Dynamic Line Rating (DLR) climate stress-test showcase, built on Databricks Free Edition. It ingests raw climate, energy, and environmental data from multiple public sources, standardises it, and transforms it into analysis-ready fact tables stored in the `climate_energy_demand` Unity Catalog. On top of the platform sits the DLR feature: a physics-based model of transmission-line capacity under weather and climate stress, served to business users through AI/BI dashboards. The pipeline covers 200+ locations, listed in a reference locations file in a Unity Catalog Volume.

The project targets **Physical Climate Risk (PCR)** analysis: combining weather observations, climate projections, energy demand, and land-use indicators into tables ready for AI/BI Dashboards and Genie natural language queries. In one sentence: the pipeline turns scattered public climate data into governed tables, the DLR model turns those tables into grid and financial insight, and the dashboards turn that insight into something a business reader can act on.

New to the project? Start with the [Project Summary](#project-summary), then [How the Pipeline Works](#how-the-pipeline-works), then use the [Documentation Map](#documentation-map) to go deeper.

## Project Summary

The project delivers three things, each building on the last:

1. **A climate data platform (Bronze → Silver → Gold).** Five ingestion streams (Open-Meteo historical weather and CMIP6 projections, NOAA GSOD station observations, OWID energy, FAOSTAT land/forest/temperature) covering 200+ locations and 180+ countries, standardised into governed Delta tables by config-driven orchestrators, with watermark-based incremental loads and a pytest validation suite that runs as a Databricks job.

2. **A dual-orchestration architecture.** Silver and Gold run side by side in two lanes: the notebook-orchestration production baseline, and a Lakeflow Spark Declarative Pipeline (SDP) migration lane. Both lanes call the same transform functions and have been validated to produce identical rows across all 19 tables. The comparison, coexistence rules, and exit criteria are in [Pipeline Approaches](pipeline_approaches.md).

3. **A DLR climate stress-test feature and showcase (SDP-native).** A Dynamic Line Rating model (IEEE 738 steady-state heat balance) that compares weather-aware dynamic line ratings against fixed static ratings for Germany, Belgium, and Singapore (2010–2026), monetises the unlocked grid capacity as avoided congestion costs, overlays wildfire exposure from a FAOSTAT forest-resilience score, and projects heat-throttling days to 2040 under three warming scenarios scored with the UN climate-risk equation (Hazard × Exposure × Vulnerability). It is served by a published two-page AI/BI dashboard: *The DLR Business Case* and *Operations & Climate Risk*. Full methodology: [dlr_design.md](dlr_design.md).

Headline DLR results. The temperatures are real observations; the wind, solar and pricing inputs are simulated, so every money figure below illustrates the model rather than a measured saving (scope and limitations in [dlr_design.md](dlr_design.md)):

| Result | Value |
| --- | --- |
| Congestion costs avoided per year (illustrative) | ~€6.2M Belgium, ~€6.7M Singapore, ~€0.1M Germany |
| Heat-throttled days in the 2010–2026 window | 11 |
| Wildfire-critical days in the 2010–2026 window | 133, all in 2011 |
| Days the NOAA forecast could not be trusted | 192, all inside the live-observation window (2023–2025) |
| Projected heat-throttled days 2027–2040, +4°C heat-dome scenario | Germany 8, Belgium 5, Singapore 11 (baseline: 1, Belgium only) |

## How the Pipeline Works

### The three layers

Data moves through three layers in the `climate_energy_demand` catalog, each with one responsibility and its own validation job:

1. **Bronze, raw mirror.** Every source lands in its own Delta table exactly as delivered. Nothing is cleaned or converted here, deliberately: Fahrenheit readings, non-standard headers, and source-specific missing-value flags are all preserved, and every row is tagged with `ingested_at` and `source_file`. Because the raw data is untouched, any later correction can be replayed from the originals. Open-Meteo data is pulled via API for every location in `reference_locations.csv`; OWID and FAOSTAT files are uploaded manually to the `raw_uploads` Unity Catalog Volume and then ingested into governed Delta tables, because Free Edition blocks outbound requests to their hosts; NOAA station data is fetched directly from the public NOAA servers over HTTPS. Source-by-source design: [Bronze README](pipelines/bronze/README.md).

2. **Silver, the standardisation gate.** The `silver_orchestrator` notebook processes one YAML config per table (`pipelines/silver/configs/*.yml`). For each config it reads the last watermark (a stored timestamp marking the most recent row already processed) from `silver.ingestion_audit`, extracts only new Bronze rows past that watermark, calls the table's transform function in `src/transforms/` (units to metric, wide FAO/OWID tables pivoted to one row per year per metric, shared dimensions built once for every pipeline), MERGEs the result into the target on the config's merge keys, and writes the new watermark back to the audit table. Each config runs inside its own error handling, so one failing table is logged and skipped while the rest complete. Adding a Silver table means adding a YAML file: the orchestrator discovers it automatically. Deep detail: [Silver README](pipelines/silver/README.md), [silver design doc](pipelines/silver/docs/design_doc.md), and the [architecture decision records](pipelines/silver/docs/architectural_decisions.md).

3. **Gold, rebuilt analytics.** The `gold_orchestrator` reads `pipelines/gold/configs/*.yml` and rebuilds every fact and dimension table from Silver on each run. Rebuilding rather than upserting is deliberate: Gold tables are derived aggregates with no independent truth of their own, so Silver remains the single source of truth and Gold can never drift from it. Deep detail: [Gold README](pipelines/gold/README.md), [gold design doc](pipelines/gold/docs/design_doc.md), and [architecture logic](pipelines/gold/docs/architecture_logic.md).

4. **The DLR feature, SDP-native.** The DLR climate stress-test feature is built entirely in the SDP lane: four silver materialized views (tables the pipeline keeps refreshed automatically: `micro_climate_synthetic_vectors`, `dim_country_historical_trust_metadata`, `weather_historical_anomaly_daily`, `climate_stress_scenarios_daily`) and two gold facts (`fct_dlr_operational_efficiency_daily`, `fct_dlr_climate_stress_horizon_annual`), added to the `silver_gold_sdp` pipeline as the source files `pipelines/silver/dlr_microclimate_sdp.py` and `pipelines/gold/dlr_gold_sdp.py`. It has no notebook-lane counterpart and sits outside the 19-table parity exit criteria by design. Methodology and dataset specs: [dlr_design.md](dlr_design.md); how it fits the lane architecture: [Pipeline Approaches](pipeline_approaches.md).

Why layers at all: each layer depends only on the one before it, any layer can be rerun independently, and validation jobs run between layers so problems surface where they were caused.

### Two ways to run Silver and Gold

Silver and Gold can be run through either of two orchestration approaches, deployed side by side and writing to separate schemas so they never conflict:

* **Notebook orchestration** (the production baseline today): config-driven orchestrator jobs as described above, with manual watermarks in `ingestion_audit`, per-layer scheduling, failure isolation per table, and monitoring through the `pipeline_runs` table.
* **Lakeflow Spark Declarative Pipelines (SDP)** (the strategic direction): the same tables declared in a single pipeline definition, `silver_gold_sdp`. Instead of orchestrating each step by hand, each table is declared once and the platform works out how and when to build it, writing to the `silver_sdp` and `gold_sdp` schemas. Automatic checkpointing replaces the manual watermarks, and the built-in SDP event log replaces the audit tables. The lane's first full update completed in October 2026 and row-count and content parity with the notebook lane is validated across all 19 table pairs.

Both approaches call the same `src/transforms/*.py` functions, so a bug fix in a transform benefits both at once. This is a staged migration with explicit exit criteria, not an open-ended choice: SDP is the intended end state for Silver and Gold, while Bronze stays notebook-based in both because its ingestion is REST-API driven. The DLR feature is SDP-native only. The full comparison, coexistence rules, exit criteria, and when to use which: [Pipeline Approaches](pipeline_approaches.md). The decision record: [ADR #7](pipelines/silver/docs/architectural_decisions.md#7-dual-pipeline-approaches-notebook-orchestration-and-sdp-coexistence).

### How the jobs are wired

Everything above is deployed from `databricks.yml`: a single configuration file, called a Declarative Automation Bundle, that describes every job, pipeline and dashboard in the project.

* `project_bootstrap` runs once and creates the catalog, all schemas (including `silver_sdp` and `gold_sdp`), and the audit and monitoring tables. It is idempotent and verified by `setup/verify_bootstrap.sql`.
* Each layer has independent load and validation jobs (`silver_data_load`, `silver_validation`, `gold_data_load`, `gold_validation`, `bronze_validation`) that can be triggered individually.
* `climate_data_pipeline` chains the full sequence: bronze validate, silver load, silver validate, gold load, gold validate. Bronze validation uses `run_if: ALL_DONE`, so stale Bronze data is flagged without blocking the rest of the run.
* The SDP pipeline `silver_gold_sdp` is created by the same deploy (source files: `silver_sdp.py`, `gold_sdp.py`, `dlr_microclimate_sdp.py`, `dlr_gold_sdp.py`) and started manually from the Pipelines page.
* The AI/BI dashboards are bundle resources in `dashboard/`: the pipeline monitoring dashboard and the DLR Climate Stress-Test Showcase.

The job table with when-to-use guidance is in [Getting Started](#getting-started).

## Data Sources

The pipeline draws on four public data providers across five ingestion streams, each covering a distinct facet of physical climate risk:

| Source | What it provides | Why it's here |
| --- | --- | --- |
| **Open-Meteo** (Historical + Incremental) | Daily temperature data (max, min, mean) for 200+ locations, 2010 to present | Foundation for current-condition analysis and the weather side of the daily energy-demand join |
| **NOAA GSOD** | Daily weather station observations (221 stations, 2023 to August 2025) | Independent ground truth: validates the Open-Meteo model per station in `fct_ground_truth_verification_daily` |
| **OWID Energy** | National electricity demand and generation, population, and GDP (220 countries, annual 2010-2025) | The energy side of the daily energy-demand join in `fct_energy_demand_daily` |
| **FAOSTAT** | Land cover (189 countries, 2010-2024), forest inventory (2010-2025), and temperature change (186 countries, 2010-2025) | Connects climate trends to land-use and ecological resilience outcomes |
| **Open-Meteo CMIP6** | Daily climate projections from 7 models (200+ locations, 2020-2040) | Extends the temperature analysis from measured history to projected futures |

Together these sources span weather observations, climate projections, energy demand, and land/forest indicators: the four pillars of a Physical Climate Risk analytics layer.

The DLR feature adds one deliberately synthetic component, `silver_sdp.micro_climate_synthetic_vectors` (wind speed, solar radiation and congestion penalty, one row per country per day), because no public dataset provides this weather physics at daily detail. Everything else in the feature builds on the real ingestion layers above. The synthetic scope and its rationale: [dlr_design.md](dlr_design.md).

## What the Gold Layer Answers

The Gold layer distils standardised Silver data into analysis-ready fact and dimension tables. Each fact table is designed to drive a specific class of analytical question:

| Gold table | Analytical use case |
| --- | --- |
| `fct_energy_demand_daily` | Joins national electricity consumption with daily weather data (180 countries; one row per country per day, 2010-2025) to analyse how temperature extremes drive heating and cooling demand; identify energy-vulnerable periods and regions |
| `fct_temp_change_annual` | Annual temperature-change indicators by country (186 countries, 2010-2025); supports warming trend and anomaly tracking against baselines |
| `fct_land_cover_annual` | Annual land cover area by category (189 countries, 2010-2024); enables deforestation and urbanisation tracking by comparing years |
| `fct_forest_resilience_annual` | Combines forest inventory with temperature signals to score forest resilience under climate stress (179 countries, 2010-2025) |
| `fct_ground_truth_verification_daily` | Compares modelled Open-Meteo weather against NOAA station observations (221 stations, 2023 to August 2025) to quantify model reliability per country and station |
| `fct_dlr_operational_efficiency_daily` (gold_sdp) | Country-day DLR operational efficiency for Germany, Belgium, Singapore (2010-2026, 18,321 rows): trust-gated dynamic vs static line ratings, unlocked capacity in MW, congestion costs avoided, and the wildfire overlay |
| `fct_dlr_climate_stress_horizon_annual` (gold_sdp) | Country-scenario-year projections (2027-2040, 126 rows): projected heat-throttling days and the UN climate-risk score (Hazard × Exposure × Vulnerability) with its pillar components, under three warming scenarios |
| `dim_stations`, `dim_locations`, `dim_date`, `dim_koppen_zones` | Shared dimensions enabling cross-fact-table drill-downs by station, geography, time period, and Köppen climate zone (the standard climate classification system, used here to group regions by climate type for comparative analysis) |

The DLR feature also reads one silver table directly: `silver_sdp.dim_country_historical_trust_metadata` (per-country base line ratings and NOAA trust constants, computed from the ground-truth verification fact rather than hardcoded).

## The DLR Showcase Dashboard

The DLR feature is presented in a published AI/BI dashboard, *DLR Climate Stress-Test Showcase*, deployed from `dashboard/` as a bundle resource:

* **Page 1, The DLR Business Case:** per-country congestion-costs-avoided counters, the static-vs-dynamic rating comparison, costs by country, and unlocked capacity over time. Every widget carries its calculation in plain language.
* **Page 2, Operations & Climate Risk:** day-to-day operational risk (heat-throttled days, NOAA forecast trust), the 2027-2040 climate stress horizon under a warming-scenario filter (vulnerability score, projected throttling days, pillar factors), and wildfire co-exposure.

Global filters (country, date range) and a scenario filter drive both pages. The dashboard reads reusable measures defined once over the gold facts (metric views), so the headline numbers on screen and the tables underneath can never disagree.

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

`databricks.yml` declares every job, the SDP pipeline, and the dashboards in one place. A single command (`databricks bundle deploy`) reproduces the entire setup in a workspace, changes are reviewed in Git before they reach the workspace, and the manifest is the one place to consult to see what exists.

### Incremental where it pays, rebuild where it is simpler

Silver loads incrementally using watermarks stored in queryable Delta tables rather than opaque checkpoint files: data freshness can be inspected with SQL, state survives restarts, and the compute budget is spent only on new data. Gold rebuilds fully on each run because rebuilding a derived aggregate is simpler and safer than upserting it, and eliminates any possibility of drift from Silver.

For the decision records behind these choices, see the [architecture decision records](pipelines/silver/docs/architectural_decisions.md). For how these choices change on a paid tier, see the [roadmap](pipelines/silver/docs/roadmap.md).

## Testing and Validation

Every layer has a pytest suite, run as a Databricks job through `src/tests/run_all_tests_notebook`:

* **Infrastructure**: catalog, schemas, and audit tables exist.
* **Bronze**: tables exist, are populated, and weather data is fresh (7-day threshold).
* **Silver**: reasonable row counts, no null primary keys, temperatures genuinely in Celsius, no duplicate primary keys, the wide-to-long normalisation worked, and watermarking is functioning.
* **Gold**: fact tables exist, business logic holds (DSI ≥ 0, carbon density correctness), primary keys are unique, referential integrity back to Silver holds, and model-vs-observation accuracy is checked per country (Greece's known station-grain bias is documented as a warning; see [ADR #8](pipelines/silver/docs/architectural_decisions.md#8-modelled-vs-observed-grain-mismatch-known-limitation)).

The DLR feature is validated inside the SDP pipeline and by post-build checks (specs in [dlr_design.md](dlr_design.md)): grain uniqueness (18,321 daily rows; 126 horizon rows), static/dynamic rating identity errors of exactly 0, the dynamic rating inside its 600–1,400 A clamp, every untrusted day falling back to the static rating, heatwave parity against `fct_energy_demand_daily` (0 violations, checked automatically inside the pipeline), throttle totals matching the scenario choke crossings, and a row-content checksum that is stable across refreshes (determinism of the synthetic vectors).

Validation runs are smart: they compare the last data load against the last validation run and skip when nothing has changed, which saves compute on Free Edition. The suites have caught real problems that job status alone never showed: null primary keys, a physically impossible 5,537°C maximum temperature, and duplicate rows introduced by MERGEs. The full guide, including how to run the tests and customise thresholds: [src/tests README](src/tests/README.md).

Every orchestrator and validation run also writes to `climate_energy_demand.monitoring`: `pipeline_runs` (per-task and per-table outcomes) and `test_results` (one row per test function with parsed, readable errors). The AI/BI monitoring dashboard reads these tables. Full schema and dashboard queries: [monitoring design doc](pipelines/consumption/monitoring/design_doc.md).

## Getting Started

### Prerequisites

* Databricks Free Edition workspace
* Databricks CLI installed locally, or use the Databricks UI to deploy the bundle

### Quick Start

1. **Deploy the bundle**: Run `databricks bundle deploy` from the CLI, or deploy via the Databricks UI. This creates the jobs, the SDP pipeline, and the dashboards defined in `databricks.yml`.

2. **Bootstrap infrastructure**: Run the `project_bootstrap` job once. This creates the `climate_energy_demand` catalog, schemas (bronze, silver, gold, silver_sdp, gold_sdp, monitoring), and audit and monitoring tables. It is idempotent (`IF NOT EXISTS`).

3. **Ingest Bronze data**: Run the Bronze ingestion notebooks. Open-Meteo notebooks pull data via API automatically. For OWID, NOAA, and FAOSTAT, download the source files and upload them to `/Volumes/climate_energy_demand/bronze/raw_uploads/`, then run the corresponding ingestion notebook.

4. **Run the pipeline**: Choose one:
   * **Full end-to-end**: Trigger `climate_data_pipeline` (chains: bronze validate, silver load, silver validate, gold load, gold validate), then start the SDP pipeline `silver_gold_sdp` from the Pipelines page for the SDP lane (including the DLR feature).
   * **Layer-by-layer**: Run individual jobs as needed. For example, `silver_data_load` then `silver_validation` when Bronze data has changed, or `gold_data_load` then `gold_validation` when Silver transforms have been updated.

5. **Explore the results**: Open the DLR Climate Stress-Test Showcase or the monitoring dashboard, or query the Gold tables directly via SQL.

### Pipeline Jobs

| Job | What it does | When to use |
| --- | --- | --- |
| `climate_data_pipeline` | Chains: bronze validate, silver load, silver validate, gold load, gold validate | Full end-to-end run, CI/CD |
| `bronze_validation` | Runs bronze layer data quality tests | After new bronze data uploads |
| `silver_data_load` + `silver_validation` | Silver orchestrator only, then validation | When bronze data changed |
| `gold_data_load` + `gold_validation` | Gold orchestrator only, then validation | When re-running gold transforms |
| `silver_gold_sdp` (SDP pipeline) | Declarative Silver + Gold rebuild, including the DLR feature | SDP-lane updates; started from the Pipelines page |

The modular jobs are decoupled; trigger each manually. The full pipeline chains everything with task dependencies so a single trigger runs all steps in sequence. Bronze validation uses `run_if: ALL_DONE` so stale data is flagged without blocking the pipeline.

## Intelligence Layer

**What is built:**

* Every Silver and Gold table has column-level comments declared in YAML configs. The orchestrators register these comments to Unity Catalog automatically, giving Genie the semantic context it needs to interpret the tables.
* The DLR Climate Stress-Test Showcase: a published two-page AI/BI dashboard over the DLR gold facts (business case + operations & climate risk), with global country and date filters and a warming-scenario filter.
* A pipeline monitoring AI/BI dashboard over `pipeline_runs` and `test_results`, deployed as a bundle resource.
* The DLR feature's datasets are exposed as metric views, so each measure is defined once with the data rather than separately in every chart that uses it.

**What can be done next:**

* Set up a Genie space pointed at the Gold tables (and the DLR facts). The column comments are already in place, so Genie can answer natural language questions like "show the five-year warming trend for coastal regions" once a space is created.
* Add curated Genie instructions or example queries to improve answer quality for domain-specific questions.

## Repository Structure

```
climate-analytics
├── databricks.yml                    # Declarative Automation Bundle manifest
├── dlr_design.md                     # DLR feature design: physics, parameters, dataset specs, validation
├── pipeline_approaches.md            # Notebook vs SDP comparison, coexistence rules, exit criteria, DLR pipeline
├── setup/                            # Infrastructure bootstrap SQL scripts
├── pipelines/
│   ├── bronze/                       # Ingestion notebooks (source-specific)
│   │   └── notebooks/
│   ├── silver/                       # Standardisation and cleaning
│   │   ├── configs/                  # YAML declarative table definitions
│   │   ├── docs/                     # Design, architectural decisions, roadmap
│   │   ├── silver_orchestrator       # Centralised execution engine (notebook)
│   │   ├── silver_sdp.py             # SDP source file (declarative silver tables)
│   │   ├── dlr_microclimate_sdp.py   # DLR feature: silver MVs (mock vectors, trust dim, anomaly helper, scenarios)
│   │   └── setup_silver.sql          # Creates silver.ingestion_audit table
│   ├── gold/                         # Aggregated analytics fact tables
│   │   ├── configs/                  # YAML table definitions (facts + dimensions)
│   │   ├── docs/                     # Design doc and architecture logic
│   │   ├── source_to_target_mappings/  # Lineage CSVs (Silver to Gold)
│   │   ├── gold_orchestrator         # Config-driven transformation runner (notebook)
│   │   ├── gold_sdp.py               # SDP source file (declarative gold materialized views)
│   │   ├── dlr_gold_sdp.py           # DLR feature: gold facts (daily efficiency, climate stress horizon)
│   │   └── setup_gold.sql            # Creates gold.ingestion_audit table
│   └── consumption/
│       └── monitoring/               # Monitoring table setup and dashboard design doc
├── src/
│   ├── transforms/                   # Domain-specific transformation modules
│   ├── common/                       # Shared utilities (audit, shared logic)
│   └── tests/                        # pytest suites (bronze, silver, gold, infrastructure, lane parity)
├── dashboard/                        # AI/BI dashboards (.lvdash.json): monitoring + DLR showcase
├── requirements.txt                  # Python dependencies
└── README.md
```

- `dlr_design.md`: the DLR feature design document — IEEE 738 physics, the locked parameter registry, dataset specifications, validation expectations, and limitations.
- `setup/`: Two SQL scripts. `project_infrastructure.sql` creates the `climate_energy_demand` catalog and all schemas (bronze, silver, gold, silver_sdp, gold_sdp, monitoring). `verify_bootstrap.sql` checks that the infrastructure exists before pipeline jobs are run.
- `pipelines/bronze/`: Source-specific ingestion notebooks. Data is driven by `reference_locations.csv` stored in the `/Volumes/climate_energy_demand/bronze/raw_uploads/` Unity Catalog Volume. See [Bronze README](pipelines/bronze/README.md) for the full source catalogue.
- `pipelines/silver/`: YAML configs, the orchestrator notebook, the SDP sources (`silver_sdp.py`, `dlr_microclimate_sdp.py`), and `setup_silver.sql` (creates the `silver.ingestion_audit` table for watermark tracking). See [Silver README](pipelines/silver/README.md) for table details.
- `pipelines/gold/`: YAML configs, the orchestrator notebook, source-to-target mapping CSVs, the SDP sources (`gold_sdp.py`, `dlr_gold_sdp.py`), and `setup_gold.sql` (creates the `gold.ingestion_audit` table for watermark tracking). See [Gold README](pipelines/gold/README.md) for table details.
- `pipelines/consumption/monitoring/`: Monitoring table setup and the dashboard design doc.
- `src/transforms/`: Six Python modules: `energy.py` (energy demand transforms), `weather.py` (weather observation and projection transforms), `climate.py` (temperature change transforms), `nature.py` (land cover and forest resilience transforms), `quality.py` (data quality checks), and `common.py` (shared transform helpers).
- `src/common/`: `audit_utils.py` (watermark tracking and audit-table writes) and `shared_logic.py` (common transformation helpers reused across domains).
- `src/tests/`: `test_infrastructure.py` (bootstrap verification), `test_bronze_tables.py`, `test_silver_tables.py`, `test_gold_tables.py`, `test_lane_parity.py` (notebook vs SDP lane comparison), `test_shared_logic.py`, and `test_audit_utils.py`. A `run_all_tests_notebook` notebook is provided for running tests inside Databricks.
- `dashboard/`: AI/BI Dashboard definitions deployed as bundle resources (pipeline monitoring and the DLR Climate Stress-Test Showcase).
- `requirements.txt`: Python libraries required for the project.

## Documentation Map

Every document in the repository, one line each:

| Document | What it covers |
| --- | --- |
| [DLR design doc](dlr_design.md) | DLR feature methodology: IEEE 738 physics, parameter registry, dataset specs, validation, limitations |
| [Pipeline Approaches](pipeline_approaches.md) | Notebook orchestration vs SDP: comparison, coexistence rules, exit criteria, and the DLR feature pipeline |
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
