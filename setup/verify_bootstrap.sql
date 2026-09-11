-- Bootstrap Verification: Check all infrastructure objects exist
-- Infrastructure validation is now handled by test_infrastructure.py (runs via validation notebook)
-- This SQL remains as a quick post-bootstrap sanity check.

SELECT 'catalog' AS component, catalog_name AS object_name, 'OK' AS status
FROM climate_energy_demand.information_schema.schemata
WHERE catalog_name = 'climate_energy_demand'
LIMIT 1;

SELECT 'schema' AS component, CONCAT(catalog_name, '.', schema_name) AS object_name, 'OK' AS status
FROM climate_energy_demand.information_schema.schemata
WHERE catalog_name = 'climate_energy_demand'
  AND schema_name IN ('bronze', 'silver', 'gold', 'monitoring')
ORDER BY schema_name;

SELECT 'table' AS component, CONCAT(table_catalog, '.', table_schema, '.', table_name) AS object_name, 'OK' AS status
FROM climate_energy_demand.information_schema.tables
WHERE table_catalog = 'climate_energy_demand'
  AND (
    (table_schema = 'silver' AND table_name = 'ingestion_audit')
    OR (table_schema = 'gold' AND table_name = 'ingestion_audit')
    OR (table_schema = 'monitoring' AND table_name IN ('pipeline_runs', 'test_results'))
  )
ORDER BY object_name;