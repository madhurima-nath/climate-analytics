
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.monitoring;

-- TEST_RESULTS: Individual test outcomes
-- One row per test function per execution
-- Purpose: Track which tests ran, their status, and clean error details for dashboard visibility
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

-- PIPELINE_RUNS: Job-level execution tracking
-- One row per task execution (orchestrator or validation)
-- Purpose: Track what jobs ran, orchestrator outcomes (per-table detail), and link to test executions
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