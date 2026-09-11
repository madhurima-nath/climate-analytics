# test_infrastructure.py
# Purpose: Validate that all required infrastructure objects exist (catalog, schemas, tables, volumes)
# Replaces the setup_status table — results flow into test_results via the validation notebook
# Run via: pytest test_infrastructure.py -v --tb=short --assert=plain --import-mode=importlib

import pytest

CATALOG = "climate_energy_demand"

# Required schemas
REQUIRED_SCHEMAS = ["bronze", "silver", "gold", "monitoring"]

# Required tables (schema.table format, relative to catalog)
REQUIRED_TABLES = [
    "monitoring.pipeline_runs",
    "monitoring.test_results",
    "silver.ingestion_audit",
    "gold.ingestion_audit",
]

# Required volumes (schema.volume format, relative to catalog)
REQUIRED_VOLUMES = [
    "bronze.raw_uploads",
]


def _full_name(schema_table):
    return f"{CATALOG}.{schema_table}"


def test_catalog_exists():
    """The main Unity Catalog catalog exists."""
    catalogs = [row.catalog_name for row in spark.sql("SHOW CATALOGS").collect()]
    assert CATALOG in catalogs, f"Catalog '{CATALOG}' not found. Available: {', '.join(catalogs)}"


def test_schemas_exist():
    """All required schemas exist within the catalog."""
    existing = set()
    df = spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
    for row in df.collect():
        existing.add(row.schema_name)
    missing = [s for s in REQUIRED_SCHEMAS if s not in existing]
    assert not missing, f"Missing schemas in {CATALOG}: {', '.join(missing)}"


def test_required_tables_exist():
    """All required infrastructure tables exist."""
    missing = []
    for schema_table in REQUIRED_TABLES:
        full = _full_name(schema_table)
        if not spark.catalog.tableExists(full):
            missing.append(full)
    assert not missing, f"Missing tables: {', '.join(missing)}"


def test_required_volumes_exist():
    """All required UC volumes exist."""
    missing = []
    for schema_volume in REQUIRED_VOLUMES:
        schema_name, volume_name = schema_volume.split(".")
        try:
            volumes = [row.volume_name for row in 
                      spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema_name}").collect()]
            if volume_name not in volumes:
                missing.append(f"{CATALOG}.{schema_volume}")
        except Exception:
            missing.append(f"{CATALOG}.{schema_volume} (schema error)")
    assert not missing, f"Missing volumes: {', '.join(missing)}"


def test_pipeline_runs_has_orchestrator_columns():
    """pipeline_runs table has the merged orchestrator columns."""
    columns = [f.name for f in spark.table(f"{CATALOG}.monitoring.pipeline_runs").schema.fields]
    expected = ["layer", "total_configs", "configs_completed", "configs_skipped", "configs_failed"]
    missing = [c for c in expected if c not in columns]
    assert not missing, f"pipeline_runs missing columns: {', '.join(missing)}. Run ALTER TABLE to add them."
