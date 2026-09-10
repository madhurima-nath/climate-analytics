
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.monitoring;

-- Summary table for Orchestrator runs
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.orchestrator_summary (
  run_timestamp TIMESTAMP,
  layer STRING,
  total_configs INT,
  completed INT,
  skipped INT,
  failed INT,
  run_id STRING
);

-- Results table for Test suites
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.test_results (
  run_timestamp TIMESTAMP,
  layer STRING,
  category STRING,
  tests_passed INT,
  tests_failed INT,
  failed_test_names STRING,
  run_id STRING
);

-- Pipeline run status table
-- Tracks execution of bundle jobs (bootstrap, silver/gold data load, validation, full pipeline)
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.pipeline_runs (
  run_timestamp TIMESTAMP,
  job_name STRING,             -- 'project_bootstrap', 'silver_data_load', 'gold_data_load', etc.
  task_key STRING,             -- Task within the job
  status STRING,               -- 'success', 'failed', 'skipped'
  run_id STRING,               -- Databricks Job run ID
  duration_seconds INT,
  error_message STRING
);

-- Infrastructure setup status table
-- Tracks one-time bootstrap runs (catalog, schemas, audit tables, monitoring tables)
CREATE TABLE IF NOT EXISTS climate_energy_demand.monitoring.setup_status (
  run_timestamp TIMESTAMP,
  component STRING,
  object_type STRING,
  object_name STRING,
  status STRING,
  run_id STRING
);