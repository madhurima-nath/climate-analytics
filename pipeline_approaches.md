# Pipeline Approaches: Notebook Orchestration vs Declarative Pipelines

This document describes the two supported approaches for processing the Silver and Gold layers of the climate analytics platform. Both coexist in the same repository, share the same transformation logic, and can be deployed independently.

The Bronze layer (API-based ingestion) is **not** converted to SDP and remains notebook-based in both approaches. SDP consumes the Bronze Delta tables as read-only sources.

> **Position.** This is not an open-ended choice between two equivalent options. Notebook Orchestration (A) is the **production baseline** and remains authoritative for `silver`/`gold` until the migration completes. SDP (B) is the **strategic direction**, running in an isolated parallel lane (`silver_sdp`/`gold_sdp`) until the exit criteria in [Coexistence Strategy](#coexistence-strategy) are met. The SDP lane is deployed, its first full update completed on 21 October 2026, and output parity against the baseline is fully validated (row counts and content, all 19 table pairs; evidence in [Exit Criteria](#exit-criteria-when-coexistence-ends)). Quality parity (criterion 3) is validated as of 4 November 2026; criteria 2, 4 and 5 remain open. The DLR climate stress-test feature is a separate workload, covered in [The DLR Feature Pipeline (SDP-Native)](#the-dlr-feature-pipeline-sdp-native): it has no notebook counterpart and sits outside these exit criteria.

## Why the SDP Lane Exists

The platform was originally built on an assumption that later proved wrong: declarative pipelines were believed to be unavailable on Databricks Free Edition, so Silver and Gold were built as notebook orchestrators with manual watermark tracking and hand-written merge logic. That assumption no longer holds. Lakeflow Spark Declarative Pipelines are supported on Free Edition, with one material limit: one active pipeline per pipeline type.

Rather than rewrite a working, tested platform, the SDP lane was added alongside it: the same Bronze sources, the same transform functions, and separate target schemas. Bronze remains notebook-based in both approaches because its ingestion is REST-API driven, which SDP does not replace.

---

## Approach A: Notebook-Based Orchestration (Existing)

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Bronze Delta Tables (API ingestion via notebooks)              │
│  climate_energy_demand.bronze.*                                  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        │  spark.table() + watermark filter
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  silver_orchestrator.py (Notebook)                              │
│                                                                 │
│  for each config in configs/*.yml:                              │
│    1. Read YAML config (sources, transform, merge_keys)        │
│    2. Get last watermark from ingestion_audit table             │
│    3. Extract: spark.table(bronze) WHERE ingested_at > wm     │
│    4. Transform: importlib → src/transforms/{module}.py        │
│    5. Load: MERGE INTO silver target on merge_keys             │
│    6. Audit: update_audit_log(table, watermark, row_count)     │
│                                                                 │
│  → Writes to: climate_energy_demand.silver.*                    │
│  → Monitors: climate_energy_demand.monitoring.pipeline_runs    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        │  spark.table() (full read, no watermark)
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  gold_orchestrator.py (Notebook)                                │
│                                                                 │
│  for each config in configs/*.yml:                              │
│    1. Read YAML config (sources, transform, merge_keys)        │
│    2. Extract: spark.table(silver), full reload                │
│    3. Transform: importlib → src/transforms/{module}.py        │
│    4. Load: OVERWRITE gold target table                         │
│    5. Audit: update_audit_log(table, timestamp, row_count)      │
│    6. Apply column comments to Unity Catalog                    │
│                                                                 │
│  → Writes to: climate_energy_demand.gold.*                      │
│  → Monitors: climate_energy_demand.monitoring.pipeline_runs    │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Location | Role |
| --- | --- | --- |
| Orchestrator (Silver) | `pipelines/silver/silver_orchestrator` | Config-driven loop: extract, transform, MERGE, audit |
| Orchestrator (Gold) | `pipelines/gold/gold_orchestrator` | Config-driven loop: extract, transform, OVERWRITE, audit |
| YAML Configs | `pipelines/silver/configs/*.yml`, `pipelines/gold/configs/*.yml` | Declarative table definitions (sources, transform, merge_keys, params) |
| Transform Functions | `src/transforms/*.py` | Pure PySpark DataFrame transformations (shared with SDP) |
| Audit Utilities | `src/common/audit_utils.py` | Manual watermark tracking via `ingestion_audit` Delta table |
| Monitoring | `climate_energy_demand.monitoring.pipeline_runs` | Custom run-level status, duration, per-table summary |
| DABs Jobs | `databricks.yml` → `resources.jobs` | `silver_data_load`, `gold_data_load`, `climate_data_pipeline` |

### Pros

* **Independent failure isolation:** Silver can fail without blocking Gold; each job runs independently.
* **Flexible scheduling:** Each layer can be triggered on its own cadence (silver on new bronze data, gold on silver updates).
* **Config-driven extensibility:** Adding a new table requires only a new YAML file; no code changes to the orchestrator.
* **Manual watermark control:** The `ingestion_audit` table is queryable by BI tools, giving full transparency into processing state.
* **No pipeline-type limit:** Multiple jobs can run concurrently (subject to Free Edition's 5 concurrent task limit).
* **Custom monitoring:** `pipeline_runs` captures exactly the metrics the dashboard needs (status, duration, per-table counts, error summaries).

### Cons

* **Manual watermark management:** Requires `audit_utils.py` and the `ingestion_audit` table; checkpoint state must be maintained manually.
* **Hand-written MERGE logic:** Upsert logic is coded in the orchestrator; merge keys are enforced via dynamic SQL.
* **No automatic dependency resolution:** Cross-layer dependencies (gold reads silver) are managed by job task chaining, not by the pipeline engine.
* **No built-in data quality:** Validation is a separate pytest job, not integrated into the load step.
* **Full reload for Gold:** Gold tables are overwritten on every run, reprocessing the entire Silver dataset.

### When to Use

* You need **independent scheduling** for Silver and Gold (e.g., Silver runs hourly, Gold runs daily).
* You want **failure isolation** between layers: a Silver failure should not block Gold from running on existing Silver data.
* You need **custom monitoring** that writes to a specific `pipeline_runs` schema for dashboards or alerts.
* You are on a **team where multiple engineers may work on different layers** in parallel.
* You prefer **explicit, code-visible control** over every step of the pipeline.

---

## Approach B: Lakeflow Spark Declarative Pipelines (SDP)

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Bronze Delta Tables (API ingestion via notebooks, unchanged)  │
│  climate_energy_demand.bronze.*                                  │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        │  Streaming read (Delta CDF)
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│  SDP Pipeline: silver_sdp.py + gold_sdp.py                      │
│  (one active pipeline per type, Free Edition constraint)       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  @dlt.table (Silver streaming)                           │   │
│  │                                                           │   │
│  │  • spark.readStream.table(bronze)                        │   │
│  │  • src.transforms.{module}.{function}(sources, params)   │   │
│  │  • Automatic checkpointing (replaces manual watermarks)  │   │
│  │  • Merge keys declared in APPLY CHANGES or table def     │   │
│  │                                                           │   │
│  │  → Writes to: climate_energy_demand.silver_sdp.*         │   │
│  └───────────────────────┬─────────────────────────────────┘   │
│                          │                                       │
│  │  @dlt.table (Gold, batch read)                          │   │
│  │                                                           │   │
│  │  • dlt.read(silver_sdp_table), full read                 │   │
│  │  • src.transforms.{module}.{function}(sources, params)   │   │
│  │  • Automatic dependency resolution (waits for silver)    │   │
│  │  • Refresh on schedule or on upstream completion           │   │
│  │                                                           │   │
│  │  → Writes to: climate_energy_demand.gold_sdp.*           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Monitoring: event_log('<pipeline-id>'), built-in, queryable  │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Location | Role |
| --- | --- | --- |
| SDP Source (Silver) | `pipelines/silver/silver_sdp.py` | `@dlt.table` declarations for each silver table: streaming reads plus `dlt.apply_changes` upserts, with `weather_historical` as a materialized view |
| SDP Source (Gold) | `pipelines/gold/gold_sdp.py` | `@dlt.table` declarations (batch reads) that create materialized views for each gold table |
| Transform Functions | `src/transforms/*.py` | Reused with backward-compatible changes: the same functions called from SDP table definitions, with optional parameters for streaming-safe behaviour |
| Pipeline Resource | `databricks.yml` → `resources.pipelines` | Single pipeline definition referencing both source files |
| Monitoring | SDP event log (Delta table) | Built-in: flow progress, data quality, lineage, errors |

### What SDP Replaces

| Notebook Approach | SDP Equivalent |
| --- | --- |
| Orchestrator loop (config iteration) | Declarative table/MV definitions (one per table) |
| `audit_utils.py` (manual watermarking) | Automatic SDP checkpointing |
| Hand-written MERGE SQL | Native SDP upsert via `APPLY CHANGES` or streaming table merge |
| `pipeline_runs` monitoring table | SDP event log (queryable via `event_log()`) |
| Job task chaining (silver → gold) | Automatic cross-layer dependency resolution |
| Separate pytest validation job | `@dlt.expectations` inline data quality rules |

### What SDP Keeps

| Component | How it is reused |
| --- | --- |
| `src/transforms/*.py` (6 modules) | Imported and called inside `@dlt` table functions; optional parameters added for the SDP lane, notebook defaults preserved |
| `src/common/shared_logic.py` | Imported as needed, unchanged |
| Bronze ingestion notebooks | Remain as Lakeflow Job tasks (API calls) |
| `setup/project_infrastructure.sql` | Still creates catalog, schemas, monitoring tables |
| YAML configs | Kept as metadata reference; logic moves into SDP source code |

### SDP Lane Adaptations

Sharing the batch-written transforms with a declarative pipeline required three adaptations, all validated in the first full run:

* **Dataset type follows source semantics.** A table function reading with `spark.readStream` becomes a streaming table; one reading with `dlt.read`/`spark.read` becomes a materialized view. `weather_historical` is an MV in this lane because its transform uses window functions that streaming flows do not support. A consequence to remember: changing a dataset's type (streaming table to MV) on the same name requires manually dropping the old artifact, because SDP rejects the change with `CANNOT_CHANGE_DATASET_TYPE`.
* **Stream-incompatible logic is parameterised, not forked.** `create_dim_stations` runs here with `skip_dedup=True`, the window-based deduplication replaced by `dlt.apply_changes` keys; the notebook orchestrator keeps the full batch logic through the same function and its default flags.
* **UDFs from shared modules are rebound by value.** Cloudpickle serialises a UDF that wraps a module-level function by reference, which fails on pipeline workers that cannot import `src`. `gold_sdp.py` rebinds the Köppen classification UDF to a wrapper defined in the pipeline source that imports the original at call time.

### Free Edition Constraints

* **One active pipeline per pipeline type:** Silver and Gold are combined into a single SDP pipeline (separate source files within one pipeline definition).
* **Serverless compute only:** SDP runs on serverless; no cluster configuration needed.
* **Max 5 concurrent job tasks:** The SDP pipeline uses one active pipeline slot, not a job task slot.

### Pros

* **No manual watermark management:** SDP tracks progress automatically via checkpoints.
* **No orchestrator code:** The config loop, error handling, and monitoring MERGE are eliminated.
* **Automatic dependency resolution:** Gold MVs wait for their upstream Silver streaming tables.
* **Built-in data quality:** `@dlt.expectations` can replace or augment pytest validation.
* **Automatic retries and lineage:** Built into SDP without custom code.
* **Incremental processing:** Streaming tables handle incremental loads natively.
* **Unified event log:** Single queryable source for all pipeline events (progress, quality, errors, lineage).

### Cons

* **No failure isolation between layers:** If a table fails, the update is marked failed and its dependent tables are blocked; independent tables in the same update may still complete, but the update must be re-run to pick up the blocked ones.
* **No independent scheduling:** One pipeline update triggers all tables; cannot run Silver without Gold.
* **One active pipeline limit:** Free Edition allows one active SDP pipeline; cannot run separate Silver/Gold pipelines.
* **Event log learning curve:** Monitoring queries shift from the custom `pipeline_runs` schema to the `event_log()` table-valued function.
* **Less explicit control:** The declarative model hides orchestration details that the notebook approach makes visible.

### When to Use

* You want **automatic checkpointing** without maintaining a watermark table.
* You want **automatic cross-layer dependencies** without job task chaining.
* You want **built-in data quality** integrated into the load step.
* You prefer a **declarative model** where the platform handles retries, lineage, and incremental state.
* You are running **regular full-cycle updates** (all layers together) rather than independent per-layer triggers.

---

## The DLR Feature Pipeline (SDP-Native)

The DLR climate stress-test feature was born in the declarative lane. It has no notebook counterpart, it is not part of the notebook-to-SDP migration, and it sits outside the 19-table parity exit criteria: there is nothing in the notebook lane to migrate. It exists as two additional source files inside the same `silver_gold_sdp` pipeline. This section covers how the feature is wired into the pipeline; the physics, parameter values and dataset logic are documented in [dlr_design.md](dlr_design.md).

### What the feature adds

Six new datasets across two source files:

| Dataset | Type | Source file | Rows | What it holds |
| --- | --- | --- | --- | --- |
| `silver_sdp.micro_climate_synthetic_vectors` | MV | `pipelines/silver/dlr_microclimate_sdp.py` | 33,969 (3 countries, 2010-2040) | Simulated wind speed, solar radiation and congestion penalty, one row per country per day |
| `silver_sdp.weather_historical_anomaly_daily` | MV | `pipelines/silver/dlr_microclimate_sdp.py` | 18,321 | Real temperature history with a rolling 10-year monthly baseline and anomaly |
| `silver_sdp.dim_country_historical_trust_metadata` | MV | `pipelines/silver/dlr_microclimate_sdp.py` | 3 | Per-country base line ratings and NOAA trust constants, computed from `gold_sdp.fct_ground_truth_verification_daily` (2023-01-01 to 2025-08-24; never hardcoded) |
| `silver_sdp.climate_stress_scenarios_daily` | MV | `pipelines/silver/dlr_microclimate_sdp.py` | 3 scenarios × 3 countries × 14 years (2027-2040) | Real recycled historical days warmed by +2°C and +4°C for the projection horizon |
| `gold_sdp.fct_dlr_operational_efficiency_daily` | MV | `pipelines/gold/dlr_gold_sdp.py` | 18,321 | Daily dynamic vs static line ratings, unlocked capacity in MW, avoided congestion costs and the wildfire overlay |
| `gold_sdp.fct_dlr_climate_stress_horizon_annual` | MV | `pipelines/gold/dlr_gold_sdp.py` | 126 | Projected heat-throttling days and the UN climate risk score per scenario-year |

### Why everything is a materialized view

The feature never touches Bronze: every input is an already-clean silver or gold table, read in batch. Streaming tables and APPLY CHANGES exist for incremental upserts from Bronze, and none of that applies here. Materialized views provide the two things the feature actually needs: automatic refresh when upstream datasets change, and automatic dependency ordering, where each gold fact waits for the silver views it reads with no job chaining. The lane-wide rule from [Approach B](#approach-b-lakeflow-spark-declarative-pipelines-sdp) applies here too: a dataset's type cannot be changed in place (`CANNOT_CHANGE_DATASET_TYPE`); the old artifact must be dropped first.

### SDP capabilities the feature uses

* **Inline data quality:** a warn-mode expectation asserts heatwave parity. The daily fact recomputes the heatwave rule independently, and the expectation checks it against `gold_sdp.fct_energy_demand_daily` over their shared 2010-2025 window (0 violations). In the notebook lane this would be a separate pytest run; here it is part of the load step and visible in the event log.
* **Automatic dependency resolution:** the gold facts read silver views and other gold facts; SDP builds them in the right order without task chaining.
* **One event log:** progress, expectation metrics and errors for the feature land in the same `event_log()` as the rest of the pipeline, so the migration lane and the DLR feature are monitored identically.
* **Deterministic synthetic data:** the pseudo-noise in the vectors derives from a hash of (country, date, channel), never `rand()`. Refreshes are idempotent: the same Bronze snapshot always produces the same rows, which is what makes cross-run checks meaningful.

### Deployment and verification

Both source files are declared alongside `silver_sdp.py` and `gold_sdp.py` in the single `silver_gold_sdp` pipeline resource in `databricks.yml`, under the same Free Edition one-active-pipeline constraint. The feature deploys with the same bundle command and refreshes with the same update; there is no separate job to schedule.

Post-refresh checks (the full list with expected values is section 9 of [dlr_design.md](dlr_design.md)): 33,969 vector rows with wind inside 0.6-8.0 m/s and penalties at or above €40; 18,321 unique country-days in the daily fact with dynamic ratings inside the 600-1,400 A clamp and exactly equal to static whenever the forecast is untrusted; 126 horizon rows, with the Extreme +4°C scenario crossing the choke threshold 8 times in Germany, 11 in Singapore and 5 in Belgium.

---

## Comparison Summary

| Dimension | Notebook Orchestration (A) | Declarative Pipelines / SDP (B) |
| --- | --- | --- |
| **State management** | Manual watermarks (`ingestion_audit` table) | Automatic SDP checkpoints |
| **Cross-layer deps** | Job task chaining (`depends_on`) | Automatic (SDP resolves graph) |
| **Failure isolation** | Per-layer (Silver and Gold independent) | Per-pipeline (a failed table blocks its dependents and fails the update) |
| **Scheduling** | Per-layer (independent triggers) | Per-pipeline (one update triggers all) |
| **Data quality** | Separate pytest validation job | `@dlt.expectations` inline + optional pytest |
| **Monitoring** | `pipeline_runs` custom table | `event_log()` built-in table |
| **Merge/upsert** | Hand-written MERGE SQL in orchestrator | Native SDP `APPLY CHANGES` / streaming merge |
| **Gold load strategy** | Full overwrite per run | Materialized view refresh |
| **Free Edition limit** | 5 concurrent job tasks | 1 active pipeline per type |
| **Code to maintain** | Orchestrator + configs + audit utils | SDP source files (configs become code) |
| **Transform functions** | `src/transforms/*.py` (shared) | `src/transforms/*.py` (shared, unchanged) |
| **Bronze layer** | Notebook jobs (unchanged) | Notebook jobs (unchanged) |

---

## Coexistence Strategy

Both approaches run side-by-side in the same repository without conflict:

### Separate Target Schemas

During the coexistence period, each approach writes to its own schemas:

| Approach | Silver Target | Gold Target |
| --- | --- | --- |
| Notebook (A) | `climate_energy_demand.silver.*` | `climate_energy_demand.gold.*` |
| SDP (B) | `climate_energy_demand.silver_sdp.*` | `climate_energy_demand.gold_sdp.*` |

Both read from the same Bronze tables (read-only, no conflict). The existing dashboard and pytest suites continue reading from `silver` and `gold`, so nothing downstream changes.

### DABs Resources

The `databricks.yml` contains both resource types:

* `resources.jobs`: existing notebook jobs (`silver_data_load`, `gold_data_load`, `climate_data_pipeline`, validation jobs). Unchanged.
* `resources.pipelines`: new SDP pipeline resource (`silver_gold_sdp`). References `silver_sdp.py` and `gold_sdp.py` as source libraries.

Deploying the bundle creates both the jobs and the pipeline. You can run either or both. They do not interfere.

### Shared Code

Both approaches import the same transform functions from `src/transforms/`. Adding a new transform or fixing a bug in a transform module benefits both approaches simultaneously.

### Monitoring Coexistence

| Approach | Where to look | How to query |
| --- | --- | --- |
| Notebook (A) | `climate_energy_demand.monitoring.pipeline_runs` | `SELECT * FROM pipeline_runs WHERE layer = 'silver'` |
| SDP (B) | SDP event log | `SELECT * FROM event_log('<pipeline-id>') WHERE event_type = 'flow_progress'` |

A dashboard can combine both sources: for example, a Silver status widget from `pipeline_runs` alongside an SDP flow progress widget from `event_log()`.

### Exit Criteria: When Coexistence Ends

Coexistence ends, and the notebook orchestrators for Silver and Gold retire to reference status, when **all** of the following are evidenced:

1. **Output parity.** Row counts and checksums per table match between `silver`/`gold` and `silver_sdp`/`gold_sdp` for the same Bronze snapshot. *Evidenced by:* a per-table comparison over all 19 table pairs, each run and its result logged in the [Validation Record](#validation-record).
2. **Monitoring parity.** `event_log()` queries reproduce the run-status, duration, and per-table views the existing dashboard derives from `pipeline_runs`. *Evidenced by:* the `event_log()` queries added to the unified dashboard alongside the `pipeline_runs` widgets (see the [monitoring design doc](pipelines/consumption/monitoring/design_doc.md)).
3. **Quality parity.** SDP expectations, or the existing pytest suites pointed at the `_sdp` schemas, cover the current validation scope. *Evidenced by:* `test_silver_tables.py` and `test_gold_tables.py` executed against the `_sdp` schemas, or expectations declared in the SDP source files.
4. **Refresh behaviour validated.** A materialized view full refresh reproduces the notebook full-reload semantics, including backfills. *Evidenced by:* one update started after new Bronze data, the `_sdp` tables compared against a notebook full reload of the same data, and the outcome logged in the [Validation Record](#validation-record).
5. **Rollback documented.** Disabling the pipeline and dropping the `_sdp` schemas returns the platform to the notebook path with no residue. *Evidenced by:* the rollback procedure written in this document.

Until then, `silver` and `gold` remain the authoritative tables, and the dashboard, Genie, and tests keep reading from them.

### Validation Record

One-off checks are logged here with method and result, so that nothing runs undocumented.

* **21 October 2026, row-count parity (criterion 1).** All 19 table pairs were compared between the notebook and SDP lanes for the same Bronze snapshot. First pass: 18 of 19 exact matches; `silver.energy_metrics` alone differed (notebook 4,568 vs SDP 3,367). Diagnosis: `process_energy_metrics` recomputed batch over the same Bronze returned 3,367 rows, matching the SDP lane exactly, so the notebook table's extra 1,201 rows were legacy drift: rows with NULL `iso_code`, re-inserted on every re-upload because the orchestrator's MERGE predicate never matches NULL keys. Remediation the same day: the NULL-key rows were deleted from `silver.energy_metrics`; verified 3,367 rows with 0 NULL-key rows, identical to `silver_sdp.energy_metrics`. Result: 19 of 19 table pairs match on row counts. Content parity was completed 23-24 October 2026 (entry below); criterion 1 is fully evidenced.
* **21 October 2026, MV incremental refresh enabled.** `delta.enableRowTracking` set to `true` on `climate_energy_demand.bronze.openmeteo_weather` (verified via `SHOW TBLPROPERTIES`), in response to the SDP advisory that `silver_sdp.weather_historical` was refreshing in full. The materialized view can refresh incrementally from the next update onward.
* **23-24 October 2026, content parity (criterion 1 complete).** Full content comparison of all 19 table pairs via `test_lane_parity.py` (`run_full_comparison`): symmetric `exceptAll` over business columns (orchestrator audit columns `_loaded_at`, `_run_id`, `_updated_at` excluded) plus row counts. First pass: 15 of 19 tables identical; 4 differed, from three root causes, all fixed:
    1. `dim_stations` (3 of 221 stations): GSOD lists 3 stations under two country names with identical `ingested_at` and date ranges, so "latest per station" ties and each lane broke the tie arbitrarily. Fixed by `STATION_COUNTRY_OVERRIDES` in `create_dim_stations` (ADR #10); the notebook lane was repaired by resetting the `dim_stations` watermark and re-running the silver orchestrator.
    2. `fct_forest_resilience_annual.avg_temp_c` (2,525 rows): floating-point summation residue at the ~15th digit. Fixed by 1-decimal rounding on DECIMAL arithmetic (ADR #11); plain-double rounding first left one `.X5` boundary flip (Belarus 2019: 8.5 vs 8.6), which decimal arithmetic removed.
    3. `fct_ground_truth_verification_daily` (1,385 rows): `process_fidelity_audit`'s `dropDuplicates` picked arbitrarily between the two observation rows of dual-affiliation stations (they differ only in `country`). Fixed by resolving the observation country through `dim_stations` before dedup (ADR #10).
    Final result: **19 of 19 table pairs identical** in row counts and content across both lanes. The check is repeatable via `src/tests/test_lane_parity.py` and runs in the gold validation job.
* **4 November 2026, quality parity (criterion 3 complete).** Both validation suites were executed against the SDP lane schemas. `test_silver_tables.py` and `test_gold_tables.py` were parameterised with a lane switch: the `TEST_LANE_SUFFIX` environment variable (set to `_sdp`) rewrites every table reference to `silver_sdp`/`gold_sdp` (including the gold suite's referential-integrity checks, which then read `silver_sdp`); unset, the suites keep validating the notebook lane, so the DAB validation jobs are unchanged. The audit/watermark tests are skipped in `_sdp` mode: they validate notebook-lane infrastructure (`ingestion_audit`), which SDP replaces with pipeline checkpoints. Results: silver suite 59 passed, 2 skipped (audit tests); gold suite 69 passed, 1 skipped (audit test), plus the documented Greece model-bias warning (ADR #8); 0 failures in both. How to re-run: Tests README, "Option 5: Against the SDP Lane".

---

## Decision Guide

```
                    ┌──────────────────────────────┐
                    │  Do you need independent      │
                    │  per-layer scheduling or     │
                    │  failure isolation?           │
                    └──────────┬───────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                   YES                   NO
                    │                     │
                    ▼                     ▼
            ┌──────────────┐    ┌──────────────────────┐
            │  Approach A  │    │  Do you want built-in │
            │  (Notebook)  │    │  data quality, auto   │
            └──────────────┘    │  checkpointing, and   │
                                │  dependency resolution?│
                                └──────────┬───────────┘
                                           │
                                ┌──────────┴──────────┐
                                │                     │
                               YES                   NO
                                │                     │
                                ▼                     ▼
                        ┌──────────────┐    ┌──────────────┐
                        │  Approach B  │    │  Either works │
                        │  (SDP)       │    │  (use A for   │
                        └──────────────┘    │  familiarity) │
                                            └──────────────┘
```

### Quick Reference

| If you need... | Use |
| --- | --- |
| Independent Silver/Gold triggers | Approach A (Notebook) |
| Failure isolation between layers | Approach A (Notebook) |
| Custom `pipeline_runs` monitoring schema | Approach A (Notebook) |
| Automatic checkpointing (no watermark table) | Approach B (SDP) |
| Built-in data quality expectations | Approach B (SDP) |
| Automatic cross-layer dependency resolution | Approach B (SDP) |
| Built-in lineage and event logging | Approach B (SDP) |
| DLR climate stress-test feature | Approach B only (SDP-native, no notebook lane) |
| Both running simultaneously for comparison | Both (coexistence mode) |