# Climate Analytics

## Overview

This repository is a climate data platform with a Dynamic Line Rating (DLR) grid stress-test showcase, built on Databricks Free Edition. Public climate, energy and land data is ingested into governed Delta tables in the `climate_energy_demand` Unity Catalog, the DLR feature models transmission-line capacity under weather and climate stress on top of them, and dashboards present the results. The pipeline covers 200+ locations and 180+ countries.

## What is real and what is simulated

The DLR feature mixes real and synthetic data, and the distinction matters:

* **Real:** all temperatures and weather history (Open-Meteo, NOAA), energy and generation data (OWID), forest data (FAOSTAT), and every pipeline, test and table built from them.
* **Simulated (mock):** wind speed, solar radiation and the congestion price. No public dataset provides these at daily detail, so they are generated deterministically (specification in [dlr_design.md](dlr_design.md)).

Every euro amount in this project is an illustration produced by the model, not a measured or estimated real-world saving. The DLR feature is a proof of concept.

New to the project? This page is the summary. [How the Pipeline Works](#how-the-pipeline-works) explains the build, and the [Documentation Map](#documentation-map) lists where everything else lives.

## Project Summary

The project delivers:

1. **A climate data platform.** Five ingestion streams (Open-Meteo weather, CMIP6 projections, NOAA stations, OWID energy, FAOSTAT land and forest) standardised into Bronze-Silver-Gold layers by config-driven orchestrators, with incremental loads and pytest data-quality suites.

2. **A dual-orchestration architecture.** Silver and Gold run in two side-by-side lanes: the original notebook orchestration, and a Lakeflow Spark Declarative Pipelines (SDP) migration lane. Both lanes call the same transform functions and have been validated to produce identical rows across all 19 tables ([Pipeline Approaches](pipeline_approaches.md)).

3. **A DLR grid stress-test feature and showcase.** IEEE 738 heat-balance physics comparing weather-aware dynamic line ratings against fixed static ratings (Germany, Belgium, Singapore, 2010-2026), a wildfire exposure overlay, and 2027-2040 warming-scenario projections scored with the UN climate-risk equation, presented in a two-page AI/BI dashboard. Methodology: [dlr_design.md](dlr_design.md).

Headline model outputs:

| Output | Value |
| --- | --- |
| Congestion costs avoided per year (simulated pricing, illustrative) | ~€6.2M Belgium, ~€6.7M Singapore, ~€0.1M Germany |
| Heat-throttled days, 2010-2026 | 11 |
| Wildfire-critical days, 2010-2026 | 133, all in 2011 |
| Days the weather model was flagged untrusted | 192, all within the NOAA observation window (2023-2025) |
| Projected heat-throttled days 2027-2040, +4°C scenario | Germany 8, Belgium 5, Singapore 11 (baseline: 1) |

## How the Pipeline Works

Data moves through three layers in the `climate_energy_demand` catalog, each with its own validation job:

1. **Bronze, raw mirror.** Every source lands exactly as delivered, nothing cleaned: any later correction can be replayed from the originals. Open-Meteo is pulled via API; OWID and FAOSTAT files are uploaded to a UC Volume first, because Free Edition blocks their hosts; NOAA is fetched over HTTPS.

2. **Silver, standardisation gate.** One YAML config per table drives the `silver_orchestrator`: read the last watermark (a stored timestamp of the most recent row already processed), process only new Bronze rows, transform, MERGE into Silver. A failing table is logged and skipped without stopping the others. Deep detail: [Silver README](pipelines/silver/README.md).

3. **Gold, rebuilt analytics.** The `gold_orchestrator` rebuilds every fact and dimension from Silver on each run, so Gold can never drift from it. Deep detail: [Gold README](pipelines/gold/README.md).

Why layers at all: each layer depends only on the one before it, and validation jobs run between layers so problems surface where they were caused.

Silver and Gold also run in a second, declarative lane: the same tables declared in one Lakeflow SDP pipeline (`silver_gold_sdp`), where automatic checkpointing replaces the manual watermarks. The two lanes write to separate schemas, share the same transform functions, and are validated to produce identical rows across all 19 tables. This is a staged migration with explicit exit criteria, not a permanent dual run. Full comparison: [Pipeline Approaches](pipeline_approaches.md).

The **DLR feature is SDP-native**: six datasets (four silver views, two gold facts) added to the same pipeline through `pipelines/silver/dlr_microclimate_sdp.py` and `pipelines/gold/dlr_gold_sdp.py`, with no notebook counterpart.

Everything is deployed from `databricks.yml`, a single Declarative Automation Bundle describing every job, pipeline and dashboard; one `databricks bundle deploy` reproduces the whole setup.

## Data Sources

Five public sources feed the pipeline:

| Source | What it provides |
| --- | --- |
| **Open-Meteo** | Daily temperatures (max, min, mean) for 200+ locations, 2010 to present |
| **NOAA GSOD** | Daily station observations (221 stations, 2023 to August 2025); ground truth for the weather model |
| **OWID Energy** | National electricity demand and generation, population, GDP (220 countries, 2010-2025) |
| **FAOSTAT** | Land cover, forest inventory, temperature change (186-189 countries, 2010-2025) |
| **Open-Meteo CMIP6** | Daily climate projections from 7 models (200+ locations, 2020-2040) |

The DLR feature adds one deliberately synthetic component, `silver_sdp.micro_climate_synthetic_vectors` (wind speed, solar radiation and congestion penalty, one row per country per day), because no public dataset provides this weather physics at daily detail. Everything else in the feature builds on the real sources above. Rationale: [dlr_design.md](dlr_design.md).

## The Gold Layer and the Dashboard

Gold facts answer one analytical question each: `fct_energy_demand_daily` (how temperature extremes drive national electricity demand), `fct_temp_change_annual` (warming trends), `fct_land_cover_annual` (deforestation and urbanisation), `fct_forest_resilience_annual` (forest stress), `fct_ground_truth_verification_daily` (weather-model reliability per station), plus the two DLR facts `fct_dlr_operational_efficiency_daily` and `fct_dlr_climate_stress_horizon_annual`. Shared dimensions (`dim_stations`, `dim_locations`, `dim_date`, `dim_koppen_zones`) allow drill-downs by station, geography, time and climate zone, and every table carries column comments registered to Unity Catalog, so the data is ready for Genie natural-language queries. Table-by-table detail: [Gold README](pipelines/gold/README.md).

The DLR Climate Stress-Test Showcase is a two-page AI/BI dashboard deployed as a bundle resource: **The DLR Business Case** (costs avoided, ratings comparison, unlocked capacity) and **Operations & Climate Risk** (heat-throttled days, forecast trust, warming-scenario projections, wildfire overlay), with global country, date and scenario filters.

## Testing and Validation

Each layer has a pytest suite run as a Databricks job: infrastructure, freshness, primary-key integrity, unit correctness and referential integrity. The suites have caught real defects that job status alone never showed: null primary keys, a physically impossible 5,537°C reading, and duplicate rows introduced by MERGEs. Validation runs skip when the data has not changed, saving compute on Free Edition, and every run writes to `monitoring.pipeline_runs` and `monitoring.test_results`, which feed the monitoring dashboard.

The DLR feature is additionally validated inside the pipeline: grain uniqueness, rating identity errors of exactly 0, clamp bounds, heatwave parity against `fct_energy_demand_daily` (0 violations), and a content checksum that is stable across refreshes. Full guide: [src/tests README](src/tests/README.md).

## Getting Started

Prerequisite: a Databricks Free Edition workspace and the Databricks CLI (or deploy via the UI).

1. Run `databricks bundle deploy`: creates the jobs, the SDP pipeline and the dashboards.
2. Run the `project_bootstrap` job once: creates the catalog, schemas and audit tables (idempotent).
3. Run the Bronze notebooks. Open-Meteo pulls automatically; upload OWID, NOAA and FAOSTAT files to `/Volumes/climate_energy_demand/bronze/raw_uploads/` first.
4. Run the pipeline: trigger `climate_data_pipeline` for the notebook lane, or start `silver_gold_sdp` from the Pipelines page for the SDP lane (this includes the DLR feature).
5. Explore: open the DLR Showcase or the monitoring dashboard, or query the Gold tables directly.

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

Key directories: `pipelines/bronze`, `pipelines/silver` and `pipelines/gold` (ingestion, standardisation and analytics; each layer has its own README), `src/transforms` (transform code shared by both lanes), `src/common` (audit and shared logic), `src/tests` (pytest suites, including `test_lane_parity.py` for the notebook-vs-SDP comparison), `dashboard/` (both AI/BI dashboards) and `setup/` (bootstrap SQL), plus the two root deep-dives: [Pipeline Approaches](pipeline_approaches.md) and [dlr_design.md](dlr_design.md).

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
