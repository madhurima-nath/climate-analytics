# Pipeline Approaches: Notebook Orchestration vs Declarative Pipelines

This document describes the two supported approaches for processing the Silver and Gold layers of the climate analytics platform. Both coexist in the same repository, share the same transformation logic, and can be deployed independently.

The Bronze layer (API-based ingestion) is **not** converted to SDP and remains notebook-based in both approaches. SDP consumes the Bronze Delta tables as read-only sources.

> **Position.** This is not an open-ended choice between two equivalent options. Notebook Orchestration (A) is the **production baseline** and remains authoritative for `silver`/`gold` until the migration completes. SDP (B) is the **strategic direction**, running in an isolated parallel lane (`silver_sdp`/`gold_sdp`) until the exit criteria in [Coexistence Strategy](#coexistence-strategy) are met. The SDP lane is built but has not yet been validated side-by-side against the baseline.

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
│  │  @dlt.streaming_table (Silver)                           │   │
│  │                                                           │   │
│  │  • dlt.read_stream(bronze_table)                         │   │
│  │  • src.transforms.{module}.{function}(sources, params)   │   │
│  │  • Automatic checkpointing (replaces manual watermarks)  │   │
│  │  • Merge keys declared in APPLY CHANGES or table def     │   │
│  │                                                           │   │
│  │  → Writes to: climate_energy_demand.silver_sdp.*         │   │
│  └───────────────────────┬─────────────────────────────────┘   │
│                          │                                       │
│  │  @dlt.materialized_view (Gold)                          │   │
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
| SDP Source (Silver) | `pipelines/silver/silver_sdp.py` | `@dlt.streaming_table` declarations for each silver table |
| SDP Source (Gold) | `pipelines/gold/gold_sdp.py` | `@dlt.materialized_view` declarations for each gold table |
| Transform Functions | `src/transforms/*.py` | Reused as-is: the same pure PySpark functions called from SDP decorators |
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
| `src/transforms/*.py` (6 modules) | Imported and called inside `@dlt` decorated functions, unchanged |
| `src/common/shared_logic.py` | Imported as needed, unchanged |
| Bronze ingestion notebooks | Remain as Lakeflow Job tasks (API calls) |
| `setup/project_infrastructure.sql` | Still creates catalog, schemas, monitoring tables |
| YAML configs | Kept as metadata reference; logic moves into SDP source code |

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

* **No failure isolation between layers:** If a Silver table fails, the pipeline update stops and Gold MVs in the same update will not run.
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

## Comparison Summary

| Dimension | Notebook Orchestration (A) | Declarative Pipelines / SDP (B) |
| --- | --- | --- |
| **State management** | Manual watermarks (`ingestion_audit` table) | Automatic SDP checkpoints |
| **Cross-layer deps** | Job task chaining (`depends_on`) | Automatic (SDP resolves graph) |
| **Failure isolation** | Per-layer (Silver and Gold independent) | Per-pipeline (Silver failure stops Gold) |
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

1. **Output parity.** Row counts and checksums per table match between `silver`/`gold` and `silver_sdp`/`gold_sdp` for the same Bronze snapshot.
2. **Monitoring parity.** `event_log()` queries reproduce the run-status, duration, and per-table views the existing dashboard derives from `pipeline_runs`.
3. **Quality parity.** SDP expectations, or the existing pytest suites pointed at the `_sdp` schemas, cover the current validation scope.
4. **Refresh behaviour validated.** A materialized view full refresh reproduces the notebook full-reload semantics, including backfills.
5. **Rollback documented.** Disabling the pipeline and dropping the `_sdp` schemas returns the platform to the notebook path with no residue.

Until then, `silver` and `gold` remain the authoritative tables, and the dashboard, Genie, and tests keep reading from them.

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
| Both running simultaneously for comparison | Both (coexistence mode) |