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

## Streamlit Dashboard: External Access

### Purpose

Provide external stakeholders (non-Databricks users) with read-only access to pipeline monitoring.

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
- [ ] Update dashboard filters to include gold
- [ ] Create gold.ingestion_audit table
- [ ] Add gold watermark widget to dashboard

---

## Summary

### Current State ✅

- Monitoring tables (test_results, pipeline_runs)
- Test infrastructure (conftest.py, run_all_tests_notebook)
- Bronze, Silver, and Gold validation tests
- Error parsing (ANSI stripping, component extraction)
- Complete test tracking (all tests recorded)

### Ready to Build 🔲

- Lakeview Dashboard (all queries documented)
- Streamlit App (external access)
- Gold orchestrator logging (when gold layer complete)
- Genie space (natural language Q&A)

### Design Principles ✅

- Self-documenting dashboard
- Per-table visibility in error messages
- All tests visible (passed AND failed)
- Clean errors without ANSI codes
- Unified schema for all layers