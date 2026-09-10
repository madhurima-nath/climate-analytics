# Monitoring Layer: Design Decisions & Implementation Plan

## Purpose

This document captures the design decisions, reasoning, and implementation plan for the monitoring layer of the climate-analytics pipeline. The monitoring system tracks pipeline health across Bronze, Silver, and Gold layers, surfaces data quality metrics, records infrastructure setup status, and enables natural language queries via Genie.

---

## Design Decisions

### 1. Orchestrator Summary Tracking

**Decision:** Create a unified job-level summary table for Silver and Gold orchestration runs.

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.orchestrator_summary (
  run_timestamp TIMESTAMP,
  layer STRING,              -- 'silver' or 'gold'
  total_configs INT,
  completed INT,
  skipped INT,
  failed INT,
  run_id STRING               -- Links to the Databricks Job run ID
);
```

**Reasoning:**

* The orchestrator already prints summary statistics, but these are buried in notebook logs.
* Dashboards need live visibility into pipeline progress without digging through logs.
* A single table for both layers keeps the schema simple while allowing layer-specific filtering.
* Provides table-level granularity that the Databricks Jobs UI does not show.
* The `run_id` column links the monitoring record to the actual Databricks Job run, enabling joins with the Jobs API for deeper diagnostics.

**Implementation:**

* The silver orchestrator inserts a summary row at the end of each execution, capturing the job run ID via `dbutils.widgets.get("job_run_id")` (passed as a task parameter from the job definition).
* The gold orchestrator follows the same pattern.

---

### 2. Infrastructure Bootstrap Tracking

**Decision:** Log one-time infrastructure setup completion to a dedicated monitoring table.

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.setup_status (
  run_timestamp TIMESTAMP,
  component STRING,           -- 'schema' or 'table'
  object_type STRING,          -- 'SCHEMA' or 'TABLE'
  object_name STRING,          -- Fully qualified name (e.g. climate_energy_demand.silver)
  status STRING,               -- 'verified'
  run_id STRING                -- 'bootstrap' for initial setup runs
);
```

**Reasoning:**

* The `project_bootstrap` job runs once at project initialisation to create the catalog, schemas, audit tables, and monitoring tables.
* A verification task (`verify_bootstrap.sql`) checks that every required object exists in `information_schema` and logs the result to this table.
* This gives the dashboard a clear 'infrastructure readiness' indicator without re-running setup scripts.
* Idempotent by design: re-running the bootstrap job appends new verification rows rather than overwriting, providing an audit trail of when infrastructure was last validated.

**Implementation:**

* The `project_bootstrap` job in `databricks.yml` chains all setup SQL files in dependency order, with the verification task running last.
* The dashboard queries the latest rows to confirm all expected objects are present.

---

### 3. Bronze Layer Monitoring Approach

**Decision:** Skip execution logging; implement validation queries instead.

**Reasoning:**

* Bronze is manual notebook execution by design (raw data ingestion).
* Most bronze tables are static; only one table is incremental.
* Execution logging adds overhead without proportional value.
* Validation queries provide actionable insights:
  * Are all 25 expected tables present?
  * Do raw upload files match bronze table row counts?
  * Is the incremental table more than 7 days stale (needs refresh)?

**Implementation:**

* Create `bronze_validation.sql` with scheduled checks.
* Flag anomalies on the dashboard (e.g. 'Incremental table last updated 10 days ago').

---

### 4. Test Results Logging & Categorisation

**Decision:** Log test results by category (not just pass/fail count) and layer (Silver vs Gold).

**Categories:**

| Category | Test Count | Scope |
| --- | --- | --- |
| Schema Validation | ~21 tests | Table existence, population, row counts |
| Data Quality | ~15 tests | Null checks, duplicates, value ranges |
| Business Logic | ~10 tests | HDD/CDD calculations, unit conversions, H3 indexing |
| System Integrity | ~5 tests | Watermarks, merge logic, audit utilities |

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.test_results (
  run_timestamp TIMESTAMP,
  layer STRING,                -- 'silver' or 'gold'
  category STRING,             -- 'Schema Validation', 'Data Quality', etc.
  tests_passed INT,
  tests_failed INT,
  failed_test_names STRING,    -- Comma-separated list if any failed
  run_id STRING                -- Links to the Databricks Job run ID
);
```

**Value Proposition:**

* Dashboards show 'Schema Validation (21/21)' instead of generic pass/fail counts.
* Immediate diagnosis when tests fail: the category pinpoints whether the issue is structural, quality-related, or logic-related.
* The `run_id` column allows joining `test_results` with `orchestrator_summary` to trace which pipeline run caused which test failures.

---

### 5. Job Run Tracking vs Monitoring Tables

**Decision:** The monitoring tables complement, rather than duplicate, the Databricks Jobs UI.

| Aspect | Databricks Jobs UI | Monitoring Tables |
| --- | --- | --- |
| Scope | Job-level (did the job run?) | Table-level (what happened inside?) |
| Granularity | Task success/failure | Per-table status and row counts |
| Test Visibility | None | Category-level breakdown |
| Infrastructure Status | None | Bootstrap verification records |
| Dashboard Access | Manual navigation | Live SQL queries and Genie |

### 6. Bundle Job Monitoring

**Decision:** Track all bundle job executions (bootstrap, data load, validation, full pipeline) in a dedicated table.

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.pipeline_runs (
  run_timestamp TIMESTAMP,
  job_name STRING,             -- 'project_bootstrap', 'silver_data_load', etc.
  task_key STRING,             -- Task within the job
  status STRING,               -- 'success', 'failed', 'skipped'
  run_id STRING,               -- Databricks Job run ID
  duration_seconds INT,
  error_message STRING
);
```

**Reasoning:**

* The Databricks Jobs UI shows job-level status but is not queryable from SQL or dashboards.
* A `pipeline_runs` table lets the dashboard surface job success/failure alongside orchestrator internals and test results.
* Captures the bootstrap job, which is a one-time run that must succeed before any pipeline can run.
* The `run_id` links to `orchestrator_summary` and `test_results`, providing a full trace from job trigger to table-level outcome.

**Implementation:**

* Each bundle job logs a row at completion (success or failure) with duration and error message.
* The bootstrap job's `verify_and_log` task also writes to this table.
* Future: use the Databricks Jobs API or a post-job webhook to automate logging.

---

## Monitoring Dashboard: Intention & Build Guide

### Intention

The monitoring dashboard provides a single-pane view of pipeline health across all layers. It serves two audiences:

* **Data engineers** who need to diagnose failures quickly and track watermark freshness.
* **Project stakeholders** who need a high-level status overview without navigating the Jobs UI.

The dashboard queries the three monitoring tables (`orchestrator_summary`, `test_results`, `setup_status`) plus the layer-specific audit tables (`silver.ingestion_audit`, `gold.ingestion_audit`). It is designed as a Lakeview dashboard with the following widgets:

### Widget 1: Orchestrator Run Status

Displays the latest pipeline run for each layer, including completed/skipped/failed counts.

```sql
SELECT
  run_timestamp,
  layer,
  total_configs,
  completed,
  skipped,
  failed,
  run_id
FROM climate_energy_demand.monitoring.orchestrator_summary
ORDER BY run_timestamp DESC
LIMIT 10;
```

**Visualisation:** Bar chart showing completed vs skipped vs failed per run, with a run_timestamp filter.

### Widget 2: Test Results by Category

Shows pass/fail counts grouped by test category for the latest run.

```sql
SELECT
  layer,
  category,
  tests_passed,
  tests_failed,
  failed_test_names
FROM climate_energy_demand.monitoring.test_results
WHERE run_timestamp = (
  SELECT MAX(run_timestamp) FROM climate_energy_demand.monitoring.test_results
)
ORDER BY layer, category;
```

**Visualisation:** Stacked bar chart (passed vs failed) grouped by category, with a layer filter.

### Widget 3: Infrastructure Status

Confirms that all required schemas and tables exist, based on the most recent bootstrap verification.

```sql
SELECT
  component,
  object_type,
  object_name,
  status,
  run_timestamp
FROM climate_energy_demand.monitoring.setup_status
WHERE run_timestamp = (
  SELECT MAX(run_timestamp) FROM climate_energy_demand.monitoring.setup_status
)
ORDER BY component, object_name;
```

**Visualisation:** Status table with conditional formatting (green for 'verified', red for missing objects).

### Widget 4: Watermark Freshness

Tracks how recently each silver table was processed, flagging stale tables.

```sql
SELECT
  table_name,
  last_watermark,
  rows_processed,
  processed_at,
  DATEDIFF(CURRENT_DATE(), processed_at) AS days_since_last_run
FROM climate_energy_demand.silver.ingestion_audit
ORDER BY processed_at DESC;
```

**Visualisation:** Table with conditional formatting: green if `days_since_last_run <= 1`, amber if 2-7, red if > 7.

### Widget 5: Bronze Validation Summary

Surfaces bronze layer health checks (table presence, staleness, row count anomalies).

```sql
-- Placeholder: replace with bronze_validation.sql query results
-- when bronze_validation.sql is implemented
SELECT 'Bronze validation pending implementation' AS status;
```

**Visualisation:** KPI cards for table count, stale table count, and anomaly count.

### Widget 6: Bundle Job Status

Shows the latest run status for each bundle job (bootstrap, silver/gold data load, validation, full pipeline).

```sql
SELECT
  run_timestamp,
  job_name,
  task_key,
  status,
  run_id,
  duration_seconds,
  error_message
FROM climate_energy_demand.monitoring.pipeline_runs
ORDER BY run_timestamp DESC
LIMIT 20;
```

**Visualisation:** Status table with conditional formatting (green for 'success', red for 'failed', grey for 'skipped'), filterable by job name.

### Build Steps

1. Create a new Lakeview dashboard in the Databricks workspace.
2. Add each widget above as a dataset with its SQL query.
3. Configure visualisations per the guidance above.
4. Set the dashboard warehouse to the shared SQL warehouse used by the project.
5. Set a refresh schedule (recommended: every 1 hour during business hours, or trigger after pipeline runs).
6. Register the dashboard in the bundle (`databricks.yml`) as a `dashboards` resource for deployment.

---

## Implementation Plan

### Phase 1: Core Monitoring (MVP)

* `orchestrator_summary` table and orchestrator logging (done).
* `setup_status` table and bootstrap verification (done).
* `test_results` table and test notebook refactoring.
* Bronze validation queries (`bronze_validation.sql`).
* Silver audit summary queries (aggregate existing `ingestion_audit` table).
* Lakeview dashboard with all six widgets.

### Phase 2: Production-Grade

* Quality scorecard (0-100 metric per layer).
* Anomaly detection queries (volume and duration deviations).
* Comprehensive Genie instructions for self-service troubleshooting.

### Phase 3: Gold Layer Integration

* Gold orchestrator and test suite integration.
* Cross-layer lineage queries.
* Comprehensive runbook.