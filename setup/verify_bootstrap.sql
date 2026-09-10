-- Bootstrap Verification: Check all infrastructure objects exist and log to monitoring
-- This runs as the final task in the project_bootstrap job.

-- Log schema verification
INSERT INTO climate_energy_demand.monitoring.setup_status
SELECT
  current_timestamp() AS run_timestamp,
  'schema' AS component,
  'SCHEMA' AS object_type,
  CONCAT(catalog_name, '.', schema_name) AS object_name,
  'verified' AS status,
  'bootstrap' AS run_id
FROM climate_energy_demand.information_schema.schemata
WHERE catalog_name = 'climate_energy_demand'
  AND schema_name IN ('bronze', 'silver', 'gold', 'monitoring');

-- Log table verification
INSERT INTO climate_energy_demand.monitoring.setup_status
SELECT
  current_timestamp() AS run_timestamp,
  'table' AS component,
  'TABLE' AS object_type,
  CONCAT(table_catalog, '.', table_schema, '.', table_name) AS object_name,
  'verified' AS status,
  'bootstrap' AS run_id
FROM climate_energy_demand.information_schema.tables
WHERE table_catalog = 'climate_energy_demand'
  AND (
    (table_schema = 'silver' AND table_name = 'ingestion_audit')
    OR (table_schema = 'gold' AND table_name = 'ingestion_audit')
    OR (table_schema = 'monitoring' AND table_name IN ('orchestrator_summary', 'test_results', 'setup_status'))
  );