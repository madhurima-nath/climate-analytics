# Monitoring Layer: Design Documentation

## Purpose

This document describes the monitoring system for the climate-analytics pipeline. The monitoring layer tracks pipeline health across Bronze, Silver, and Gold layers, providing dashboard-ready visibility into:

- **What ran**: Which jobs, tasks, and table loads executed
- **What happened**: Per-table outcomes (loaded/skipped/failed with reasons and row counts)
- **Test results**: All validation tests with pass/fail status and clean error messages
- **Pipeline health**: Success rates, duration trends, and failure patterns

**Core Principle**: Anyone should be able to understand pipeline health from the dashboard **without reading code or digging through logs**.

---

## Monitoring Tables Schema

### 1. test_results: Individual Test Outcomes

**Purpose**: Track every test function execution with clean, dashboard-ready error details.

**Schema**:

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.test_results (
  execution_id STRING NOT NULL COMMENT 'Unique UUID per test suite execution (tracks validation runs over time)',
  run_id STRING COMMENT 'Databricks job run ID (links to pipeline_runs)',
  run_timestamp TIMESTAMP NOT NULL COMMENT 'When the test ran',
  
  -- Test identification
  suite_name STRING COMMENT 'Test suite name (e.g., Infrastructure Validation, Bronze Table Validation)',
  test_name STRING COMMENT 'Test function name (e.g., test_catalog_exists, test_bronze_data_freshness)',
  test_file STRING COMMENT 'Test file name (e.g., test_infrastructure.py)',
  layer STRING COMMENT 'Pipeline layer: bronze, silver, gold',
  
  -- Test result
  status STRING COMMENT 'Test status: passed, failed, skipped, error',
  duration_ms INT COMMENT 'Test execution duration in milliseconds',
  
  -- Clean error details (no ANSI codes, parsed for readability)
  error_type STRING COMMENT 'Exception type (e.g., NameError, AssertionError, None if passed)',
  error_message STRING COMMENT 'Clean error message without ANSI escape codes',
  error_file STRING COMMENT 'File where error occurred',
  error_line INT COMMENT 'Line number where error occurred',
  stack_trace STRING COMMENT 'Full stack trace if needed for deep debugging (optional)'
);
```

**Key Features**:
- ✅ **One row per test function** - Not aggregated, shows ALL tests (passed AND failed)
- ✅ **execution_id groups tests** - All tests from one validation run share the same execution_id
- ✅ **Clean error parsing** - ANSI codes stripped, error components extracted (type, message, file, line)
- ✅ **Dashboard-ready** - No need to parse logs or stack traces to understand failures

---

### 2. pipeline_runs: Job-Level Execution Tracking

**Purpose**: Track what jobs ran, orchestrator outcomes (per-table detail), and link to test executions.

**Schema**:

```sql
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.pipeline_runs (
  run_timestamp TIMESTAMP COMMENT 'When the task started',
  job_name STRING COMMENT 'Databricks job name (e.g., Silver: Data Load, Bronze: Data Validation)',
  task_key STRING COMMENT 'Task key within the job (e.g., run_orchestrator, run_validation_tests)',
  task_type STRING COMMENT 'Task type: orchestration (data load) or validation (test suite)',
  status STRING COMMENT 'Task status: running, completed, failed, skipped',
  run_id STRING COMMENT 'Databricks job run ID',
  duration_seconds INT COMMENT 'Task execution duration in seconds',
  error_message STRING COMMENT 'Per-table detail for orchestration (Completed: table (rows) | Skipped: table (reason) | Failed: table (error)) or summary for validation',
  layer STRING COMMENT 'Pipeline layer: bronze, silver, gold, monitoring',
  
  -- Orchestration-specific columns (data load runs)
  total_configs INT COMMENT 'Total number of table configs processed by orchestrator',
  configs_completed INT COMMENT 'Number of tables successfully loaded',
  configs_skipped INT COMMENT 'Number of tables skipped (e.g., no new data since watermark)',
  configs_failed INT COMMENT 'Number of tables that failed to load',
  
  -- Link to test results (validation runs)
  test_execution_id STRING COMMENT 'Links to test_results.execution_id when task_type = validation (allows joining to see all test details)'
);
```

**Key Features**:
- ✅ **Unified table** - Both orchestration and validation runs in one place
- ✅ **Per-table detail in error_message** - "Completed: energy_metrics (1,234 rows) | Skipped: weather (no new data) | Failed: carbon_flux (NameError)"
- ✅ **Orchestrator summary counts** - total_configs, configs_completed, configs_skipped, configs_failed
- ✅ **Links to test_results** - test_execution_id joins to test_results.execution_id for drill-down

---

## Design Decisions

### 1. Why Two Tables?

**Decision**: Use two monitoring tables (test_results + pipeline_runs) instead of three separate tables.

**Reasoning**:
- ✅ Simpler schema - One table for all job execution tracking
- ✅ Easier joins - test_execution_id directly in pipeline_runs
- ✅ Dashboard queries simpler - Single table to query for job status

### 2. Why execution_id?

**Decision**: Generate a unique UUID per test suite run, separate from run_id.

**Reasoning**:
- ✅ **Groups tests** - All tests from one validation run share the same execution_id
- ✅ **Tracks runs over time** - "Show me all tests from the 10:00 AM validation"
- ✅ **Counts validation runs** - COUNT(DISTINCT execution_id) = number of validation runs

### 3. Why Clean Error Parsing?

**Decision**: Strip ANSI codes, extract error components (type, message, file, line) at write time.

**Reasoning**:
- ✅ **Dashboard-ready** - No need to parse raw pytest output in SQL
- ✅ **Readable** - "NameError: name spark is not defined" instead of ANSI-coded output
- ✅ **Filterable** - WHERE error_type = 'AssertionError'

### 4. Why Record ALL Tests?

**Decision**: Write one row per test function, including passed tests.

**Reasoning**:
- ✅ **Dashboard shows complete picture** - "9 tests ran: 7 passed, 2 failed"
- ✅ **Verify test coverage** - "Did test_bronze_data_freshness run?"
- ✅ **Trend analysis** - "test_bronze_tables_exist has passed 100% over 30 days"

---

## Dashboard Design

### Core Principle

**Anyone should be able to understand pipeline health from the dashboard without reading code or digging through logs.**

### Original Design Concepts (Pages 1-4)

> **Note**: These were the initial multi-page design concepts. The actual dashboard was implemented as a single consolidated page — see [Implemented Dashboard](#implemented-dashboard-single-page-monitoring-lakeview) below for the final implementation.

### Page 1: Overview

#### KPI Cards

**Total Runs (Last 7 Days)**:
```sql
SELECT COUNT(DISTINCT run_id)
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE run_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

**Success Rate**:
```sql
SELECT 
  ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE run_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

**Failed Tests (Latest Run)**:
```sql
SELECT COUNT(*)
FROM climate_energy_demand.monitoring.test_results
WHERE status = 'failed'
  AND execution_id = (
    SELECT MAX(execution_id)
    FROM climate_energy_demand.monitoring.test_results
  );
```

**Avg Duration (Minutes)**:
```sql
SELECT ROUND(AVG(duration_seconds) / 60, 1) AS avg_minutes
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE task_type = 'orchestration'
  AND run_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

#### Recent Pipeline Runs

```sql
SELECT
  run_timestamp,
  job_name,
  task_key,
  task_type,
  layer,
  status,
  duration_seconds,
  CASE 
    WHEN LENGTH(error_message) > 200 THEN CONCAT(SUBSTRING(error_message, 1, 200), '...')
    ELSE error_message
  END AS error_summary,
  run_id
FROM climate_energy_demand.monitoring.pipeline_runs
ORDER BY run_timestamp DESC
LIMIT 20;
```

---

### Page 2: Data Load (Orchestration)

#### Table Load Outcomes

```sql
SELECT
  run_timestamp,
  layer,
  configs_completed,
  configs_skipped,
  configs_failed
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE task_type = 'orchestration'
  AND run_timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY run_timestamp DESC;
```

#### Orchestration Details with Per-Table Breakdown

```sql
SELECT
  run_timestamp,
  job_name,
  layer,
  total_configs,
  configs_completed,
  configs_skipped,
  configs_failed,
  duration_seconds,
  error_message AS per_table_detail
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE task_type = 'orchestration'
ORDER BY run_timestamp DESC
LIMIT 50;
```

**Key**: `error_message` contains per-table detail like:
```
Completed: energy_metrics (1,234 rows) | Skipped: weather_historical (no new data) | Failed: carbon_flux (NameError)
```

---

### Page 3: Test Results

#### ALL Tests from Latest Run

```sql
SELECT
  suite_name,
  test_name,
  layer,
  status,
  error_type,
  error_message,
  error_file,
  error_line
FROM climate_energy_demand.monitoring.test_results
WHERE execution_id = (
  SELECT MAX(execution_id)
  FROM climate_energy_demand.monitoring.test_results
)
ORDER BY 
  layer,
  suite_name,
  CASE status WHEN 'failed' THEN 0 ELSE 1 END,
  test_name;
```

#### Failed Tests Only

```sql
SELECT
  run_timestamp,
  layer,
  suite_name,
  test_name,
  error_message
FROM climate_energy_demand.monitoring.test_results
WHERE status = 'failed'
ORDER BY run_timestamp DESC
LIMIT 50;
```

---

### Page 4: Watermark Freshness

#### Silver Layer

```sql
SELECT
  table_name,
  last_watermark,
  rows_processed,
  processed_at,
  DATEDIFF(CURRENT_DATE(), DATE(processed_at)) AS days_since_last_run
FROM climate_energy_demand.silver.ingestion_audit
ORDER BY days_since_last_run DESC;
```

**Conditional formatting**:
- Green: ≤ 1 day
- Amber: 2-7 days
- Red: > 7 days

#### Gold Layer (Future)

```sql
SELECT
  table_name,
  last_watermark,
  rows_processed,
  processed_at,
  DATEDIFF(CURRENT_DATE(), DATE(processed_at)) AS days_since_last_run
FROM climate_energy_demand.gold.ingestion_audit
ORDER BY days_since_last_run DESC;
```

---

### Implemented Dashboard: Single-Page Monitoring (Lakeview)

**Dashboard Name**: Climate Analysis Dashboard

**Dashboard File**: `dashboard/Climate Analysis Dashboard.lvdash.json` (in the DABs bundle root)

**Status**: ✅ Implemented and rendering — single page ("1. Monitoring") with 4 datasets, 17 widgets

---

#### Datasets (4)

##### 1. ds_latest_pipeline_run (`ed87896b`)

Latest 2 pipeline runs per layer + task_type, with human-readable columns:

```sql
WITH ranked_runs AS (
  SELECT
    run_timestamp, job_name, layer, task_type, status, duration_seconds,
    total_configs, configs_completed, configs_skipped, configs_failed,
    error_message AS per_table_details,
    ROW_NUMBER() OVER (PARTITION BY layer, task_type ORDER BY run_timestamp DESC) as rn
  FROM climate_energy_demand.monitoring.pipeline_runs
)
SELECT
  run_timestamp, job_name, layer, task_type, status, duration_seconds,
  total_configs, configs_completed, configs_skipped, configs_failed,
  per_table_details,
  CASE WHEN LOWER(task_type) LIKE '%validation%' THEN 'Data Validation'
       WHEN LOWER(task_type) LIKE '%orchestrat%' THEN 'Data Load'
       ELSE task_type END AS `Task Type`,
  DATE_FORMAT(run_timestamp, 'yyyy-MM-dd HH:mm') AS `Run Time (UTC)`,
  CASE WHEN COALESCE(configs_failed, 0) > 0 THEN '❌ Fail' ELSE '✅ Pass' END AS `Result`
FROM ranked_runs
WHERE rn <= 2
ORDER BY run_timestamp DESC;
```

**Source table**: `climate_energy_demand.monitoring.pipeline_runs`

**Used by widgets**: Bronze Run Info, Silver Run Info, Gold Run Info (all filtered to respective layer)

---

##### 2. ds_latest_test_results (`766c9ff9`)

Latest test suite execution with stakeholder-friendly presentation: test names human-readable via INITCAP/REPLACE, status as pass/fail icons, error_type inferred from error_message when NULL for failed tests, details summarized not verbatim.

**Source table**: `climate_energy_demand.monitoring.test_results`

**Key transformations**:
- Test names human-readable via CASE statement (e.g., `test_pipeline_runs_has_orchestrator_columns` → "Monitoring Tables Schema Check", `test_catalog_exists` → "Climate Energy Demand Catalog Exists", `test_schemas_exist` → "All Required Schemas Exist (Bronze, Silver, Gold, Monitoring)", fallback uses `INITCAP(REPLACE(REPLACE(test_name, 'test_', ''), '_', ' '))`)
- Status shown as pass/fail icons (✅ Pass / ❌ Fail / INITCAP(status))
- Error Type uses source `error_type` when available; infers from `error_message` pattern when NULL for failed tests (AttributeError, AssertionError, KeyError, ValueError, TypeError, fallback "Error"); "N/A" for passing tests
- Details are short summaries — extracts description before colon for stale-tables errors, uses descriptive summary for schema errors, truncates unknown errors with ellipsis. "N/A" for passing tests. Never displays raw verbatim error messages.

**Used by widgets**: Bronze Validation, Silver Validation, Gold Validation, Infrastructure Validation (all filtered to respective layer)

---

##### 3. ds_per_table_details (`per_table_details`)

Parses the `per_table_details` string from the latest orchestration run per layer into individual table-level rows showing completed/skipped status.

**Source table**: `climate_energy_demand.monitoring.pipeline_runs`

**Key transformations**:
- Uses CTEs: `ranked_runs` → `latest_runs` → `sections` → `completed_entries` / `skipped_entries` → `all_entries`
- `REGEXP_EXTRACT` extracts "Completed:" and "Skipped:" sections from the `error_message` field
- `LATERAL VIEW EXPLODE(REGEXP_EXTRACT_ALL(...))` splits comma-separated table entries into individual rows
- Each row shows: layer, raw_status (Completed/Skipped), table name, row count or skip reason

**Used by widgets**: Silver Table Details (Latest Run), Gold Table Details (Latest Run)

---

##### 4. ds_table_kpis (`table_kpis`)

Table counts and latest refresh dates per medallion layer.

**Source table**: `climate_energy_demand.information_schema.tables`

**Key transformations**:
- Counts tables in `bronze`, `silver`, `gold` schemas (excluding `ingestion_audit`)
- Returns: layer name, table count, latest refresh date (`DATE_FORMAT(MAX(last_altered), 'yyyy-MM-dd')`)

**Used by widgets**: Bronze Count, Silver Count, Gold Count (counter widgets), Bronze Refresh, Silver Refresh, Gold Refresh (counter widgets)

---

#### Canvas Layout — Single Page ("1. Monitoring")

No top filter bar. All three layers display simultaneously in side-by-side columns.

**Row 0**: Layer headers (text widgets) — Bronze (col 0), Silver (col 4), Gold (col 8), each w4.

**KPI Counters**: Table count and latest refresh date per layer (6 counter widgets from `ds_table_kpis`).

**Rows 1-3**: Run Info (table widgets from `ds_latest_pipeline_run`) — each shows Task Type, Run Time (UTC), Result, status, duration_seconds, configs_completed, configs_skipped, configs_failed, filtered to the column's layer.

**Rows 4-7**: Per-Table Details (Silver and Gold only, from `ds_per_table_details`) — shows individual table-level completed/skipped status. Bronze omits this section because it is validation-only.

**Validation sections**: Bronze Validation (expanded, height 8), Silver Validation, Gold Validation — test results from `ds_latest_test_results`, filtered to each layer. Columns: Validation, Result, Error Type, Details.

**Infrastructure Validation** — test results from `ds_latest_test_results`, filtered to infrastructure/non-layer tests.

#### Presentation Rules

1. Column headers properly capitalized (Validation, Result, Error Type, Details) — no raw SQL field names
2. Test names human-readable via INITCAP + REPLACE (e.g., "Schemas Exist" not "test_schemas_exist"). Specific overrides for unclear names (e.g., "Monitoring Tables Schema Check" for test_pipeline_runs_has_orchestrator_columns)
3. Status shown as pass/fail icons — not raw "passed"/"failed"
4. Error Type uses source error_type when available; infers from error message pattern when NULL for failed tests (AttributeError, AssertionError, KeyError, ValueError, TypeError, fallback "Error"); "N/A" for passing tests
5. Details are short summaries — extracts description before colon for stale-tables errors, uses descriptive summary for schema errors, truncates unknown errors to 80 chars with ellipsis. "N/A" for passing tests. Never displays raw verbatim error messages.

#### Widget-Scoped Filters

Each widget filters by layer using widget-scoped predicates (no page-level filter): Bronze widgets `layer IN ('bronze')`, Silver widgets `layer IN ('silver')`, Gold widgets `layer IN ('gold')`.

#### Complete Widget Inventory (17 widgets)

| Widget Name | Type | Dataset | Description |
| --- | --- | --- | --- |
| bronze_header | text | — | Bronze layer header label |
| silver_header | text | — | Silver layer header label |
| gold_header | text | — | Gold layer header label |
| bronze_count | counter | ds_table_kpis | Bronze table count |
| silver_count | counter | ds_table_kpis | Silver table count |
| gold_count | counter | ds_table_kpis | Gold table count |
| bronze_refresh_date | counter | ds_table_kpis | Bronze latest refresh date |
| silver_refresh_date | counter | ds_table_kpis | Silver latest refresh date |
| gold_refresh_date | counter | ds_table_kpis | Gold latest refresh date |
| bronze_run_info | table | ds_latest_pipeline_run | Latest bronze pipeline runs |
| silver_metadata | table | ds_latest_pipeline_run | Latest silver pipeline runs |
| gold_metadata | table | ds_latest_pipeline_run | Latest gold pipeline runs |
| bronze_details | — | — | (Reserved, bronze has no orchestration) |
| silver_details | table | ds_per_table_details | Silver per-table load breakdown |
| gold_details | table | ds_per_table_details | Gold per-table load breakdown |
| bronze_tests | table | ds_latest_test_results | Bronze validation results |
| silver_tests | table | ds_latest_test_results | Silver validation results |
| gold_tests | table | ds_latest_test_results | Gold validation results |
| infra_tests | table | ds_latest_test_results | Infrastructure validation results |

#### Current Data State

- **Bronze** — 1 run (validation, completed, 91s). 9 test results: 7 pass, 2 fail (Bronze Data Freshness / Schemas Exist). Config counts NULL (validation task, no orchestration).
- **Silver** — 1 run (validation, completed). Per-table details NULL. 0 test results in latest execution.
- **Gold** — 0 rows (no gold layer pipeline runs logged yet).

#### Dashboard Source File Location & Bundle Registration

The dashboard `.lvdash.json` file lives in the DABs bundle at `dashboard/Climate Analysis Dashboard.lvdash.json`. All 4 SQL datasets are embedded directly in this file — no external SQL files are required. The file is tracked in Git alongside the rest of the bundle.

The dashboard is registered as a `dashboards:` resource in `databricks.yml`:

```yaml
resources:
  dashboards:
    climate_monitoring:
      display_name: "Climate Analysis Dashboard"
      file_path: "./dashboard/Climate Analysis Dashboard.lvdash.json"
      warehouse_id: "${var.warehouse_id}"
```

**Why register in the bundle (vs. Git tracking alone)?**

Git tracking alone version-controls the file, but registering it as a bundle resource adds lifecycle management:

1. **One-command deployment** — `bundle deploy` pushes the dashboard to the workspace alongside all jobs. Without YAML registration, the `.lvdash.json` must be manually re-imported/published on every change.
2. **CI/CD integration** — A single `bundle deploy` in an automated pipeline (GitHub Actions, etc.) updates everything — jobs and dashboards — with no separate dashboard step.
3. **Environment promotion** — With multiple targets (e.g., dev/prod), the bundle can deploy the same dashboard to different workspaces, potentially parameterized with different variables (e.g., a different catalog name via `${var.catalog}`).
4. **Validation** — `bundle validate --strict` catches schema/config issues in the dashboard definition before deployment, just like it does for jobs.
5. **Variable substitution** — Bundle variables (e.g., `${var.warehouse_id}`) can be referenced in the dashboard resource definition, parameterizing it per target.

**Impact on existing resources**: Adding a `dashboards:` block is independent of the `jobs:` block. `bundle deploy` re-deploys jobs only if their definitions changed; since no job entries were modified, existing pipelines remain untouched. `bundle run` is resource-specific — deploying a dashboard does not trigger any job to run.

#### Streamlit Note

The entire Lakeview dashboard design — including all 4 datasets (`ds_latest_pipeline_run`, `ds_latest_test_results`, `ds_per_table_details`, `ds_table_kpis`), the 3-column layout, KPI counters, run info tables, per-table details, validation tables with human-readable test names and summarized error details, and all SQL queries — must be replicated in the Streamlit app. See the Streamlit section below for replication requirements.

---

## Streamlit Dashboard: External Access

### Purpose

Provide external stakeholders (non-Databricks users) with read-only access to pipeline monitoring.

> **⚠️ Replication Requirement**: The entire Lakeview dashboard design — including the Layer Summary Grid (3-column layout, header badges, run info, per-table details, validation tables with human-readable test names and summarized error details), > **Replication Requirement**: The entire Lakeview dashboard design — including all 4 datasets (`ds_latest_pipeline_run`, `ds_latest_test_results`, `ds_per_table_details`, `ds_table_kpis`), the single-page 3-column layout, KPI counters (table counts + refresh dates), run info tables, per-table details, validation tables with human-readable test names and summarized error details, and all SQL queries — must be replicated in this Streamlit app. External stakeholders should see the same information and layout as the Lakeview dashboard.

### Architecture

```
Streamlit App → Databricks SQL Warehouse → Monitoring Tables
```

### Connection Pattern

```python
from databricks import sql
import os

connection = sql.connect(
    server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"]
)
```

### Key Components

```python
import streamlit as st
import pandas as pd

# KPI Cards
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Runs", total_runs)
with col2:
    st.metric("Success Rate", f"{success_rate}%")

# Recent Runs Table
st.dataframe(runs_df)

# Test Results
st.dataframe(tests_df)
```

### Deployment Options

1. **Streamlit Community Cloud** - Free hosting
2. **Databricks Apps** - Native integration (recommended)
3. **Container Hosting** - AWS/Azure/GCP

### Security

- Create service principal for app access
- Grant SELECT only on monitoring tables
- Use OAuth token stored in secrets manager

---

## Gold Layer Monitoring (Future)

### Overview

Gold layer monitoring follows the same pattern as silver:
1. **Orchestration logging** → pipeline_runs table
2. **Validation tests** → test_results table
3. **Dashboard integration** → Filter by layer='gold'

### Gold Orchestrator Logging

Add to gold orchestrator:

```python
# Build per-table detail message
summary_detail = []
for table, outcome in results.items():
    if outcome['status'] == 'completed':
        summary_detail.append(f"Completed: {table} ({outcome['rows']} rows)")
    elif outcome['status'] == 'skipped':
        summary_detail.append(f"Skipped: {table} ({outcome['reason']})")
    else:
        summary_detail.append(f"Failed: {table} ({outcome['error']})")

error_message = " | ".join(summary_detail)

# Write to pipeline_runs
spark.sql(f"""
    INSERT INTO climate_energy_demand.monitoring.pipeline_runs
    VALUES (
        TIMESTAMP '{datetime.now()}',
        'Gold: Data Load',
        'run_gold_orchestrator',
        'orchestration',
        '{"completed" if failed_count == 0 else "failed"}',
        '{job_run_id}',
        {duration_seconds},
        '{error_message.replace("'", "''")}',
        'gold',
        {len(configs)},
        {completed_count},
        {skipped_count},
        {failed_count},
        NULL
    )
""")
```

### Gold Validation Tests

Create `/src/tests/test_gold_tables.py` with:

1. **Table existence** - Verify all gold tables exist
2. **Data quality** - No orphan facts, referential integrity
3. **Business logic** - Aggregates match silver detail
4. **Dimension validity** - All dimension keys valid

### Dashboard Integration

No schema changes needed! Existing queries work:

```sql
SELECT *
FROM climate_energy_demand.monitoring.pipeline_runs
WHERE layer = 'gold'  -- Just add this filter
```

### Implementation Checklist

- [ ] Add orchestration logging to gold orchestrator
- [ ] Create test_gold_tables.py with ~15-20 tests
- [ ] Add gold_data_validation job to bundle
- [x] Update dashboard filters to include gold (already implemented in single-page dashboard)
- [ ] Create gold.ingestion_audit table
- [x] Add gold watermark widget to dashboard (covered by ds_table_kpis gold refresh date counter)

---

## Summary

### Current State ✅

- Monitoring tables (test_results, pipeline_runs)
- Test infrastructure (conftest.py, run_all_tests_notebook)
- Bronze, Silver, and Gold validation tests
- Error parsing (ANSI stripping, component extraction)
- Complete test tracking (all tests recorded)
- Lakeview Dashboard (single page, 4 datasets, 17 widgets — fully implemented and rendering)
  - ds_latest_pipeline_run — latest 2 runs per layer/task_type
  - ds_latest_test_results — latest test suite with human-readable names and summarized errors
  - ds_per_table_details — per-table load breakdown (completed/skipped) via REGEXP + LATERAL VIEW EXPLODE
  - ds_table_kpis — table counts and latest refresh dates per medallion layer
  - KPI counter widgets (table counts + refresh dates per layer)
  - Infrastructure Validation widget
  - Dashboard file tracked in Git at `dashboard/Climate Analysis Dashboard.lvdash.json`
  - Registered as `dashboards.climate_monitoring` resource in `databricks.yml` for bundle lifecycle management (deploy, validate, CI/CD, environment promotion)

### Ready to Build 🔲

- Streamlit App (external access)
- Genie space (natural language Q&A)

### Design Principles ✅

- Self-documenting dashboard
- Per-table visibility in error messages
- All tests visible (passed AND failed)
- Clean errors without ANSI codes
- Unified schema for all layers