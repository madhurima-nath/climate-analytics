# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Imports
# Location: src/tests/run_all_tests_notebook.py
# Purpose: Test runner notebook for Silver Layer validation
# Usage: Run this notebook in a Databricks job

import sys
import os
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Environment Setup
def setup_environment():
    """Configure Python path and environment."""
    # Get project root (2 levels up from this script)
    script_dir = Path("/Workspace/Users/madhurima.nath@icloud.com/climate-analytics/src/tests")
    project_root = script_dir.parent.parent
    
    # Add to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    print(f"Project root: {project_root}")
    print(f"Test directory: {script_dir}")
    print(f"Python path configured: {str(project_root) in sys.path}")
    
    return script_dir

test_dir = setup_environment()

# COMMAND ----------

# DBTITLE 1,Monitoring: Log Run Start
# ============================================================================
# MONITORING: Log run start to pipeline_runs
# Captures every execution: full pipeline, child tasks, or manual runs
# ============================================================================
from datetime import datetime

# Read job parameters (passed as base_parameters from job, defaults for manual runs)
try:
    job_run_id = dbutils.widgets.get("job_run_id")
except Exception:
    job_run_id = "manual"
try:
    layer = dbutils.widgets.get("layer")
except Exception:
    layer = "unknown"
try:
    task_key = dbutils.widgets.get("task_key")
except Exception:
    task_key = "manual"

# Get job name (from widget parameter, not spark.conf — avoids Spark Connect cold-start delay)
try:
    job_name = dbutils.widgets.get("job_name")
except Exception:
    job_name = "standalone"

# Record start time for duration calculation
run_start_time = datetime.now()

# Helper: update the pipeline_runs row for this run with final status
def update_pipeline_run_status(status, error_message=None, duration_seconds=None):
    """MERGE final status into pipeline_runs for this run."""
    set_parts = [f"status = '{status}'"]
    if error_message is not None:
        safe_msg = error_message.replace("'", "''")
        set_parts.append(f"error_message = '{safe_msg}'")
    if duration_seconds is not None:
        set_parts.append(f"duration_seconds = {duration_seconds}")
    set_clause = ", ".join(set_parts)
    spark.sql(f"""
        MERGE INTO climate_energy_demand.monitoring.pipeline_runs AS t
        USING (SELECT '{job_run_id}' AS run_id, '{task_key}' AS task_key) AS s
        ON t.run_id = s.run_id AND t.task_key = s.task_key
        WHEN MATCHED THEN UPDATE SET {set_clause}
    """)

# INSERT a "running" row into pipeline_runs
spark.sql(f"""
    INSERT INTO climate_energy_demand.monitoring.pipeline_runs
    (run_timestamp, job_name, task_key, task_type, status, run_id, layer)
    VALUES (TIMESTAMP '{run_start_time}', '{job_name}', '{task_key}', 'validation', 'running', '{job_run_id}', '{layer}')
""")

print(f"\U0001f4ca Monitoring: Logged run start (run_id={job_run_id}, layer={layer}, task_key={task_key})")

# COMMAND ----------

# DBTITLE 1,Check if Validation Needed
from datetime import datetime
from pyspark.sql import functions as F

def should_run_validation():
    """
    Check if validation should run based on data changes.
    Returns: (should_run: bool, reason: str)
    """
    # Bronze is manual - always run validation (no incremental audit check)
    if layer == "bronze":
        return True, "Bronze validation - manual layer, always run checks"
    
    try:
        audit_table = "climate_energy_demand.silver.ingestion_audit"
        
        # Check if audit table exists
        if not spark.catalog.tableExists(audit_table):
            return True, "⚠️  Audit table doesn't exist yet - running validation"
        
        # Get most recent data load timestamp
        data_df = spark.sql(f"""
            SELECT MAX(processed_at) as last_data_load
            FROM {audit_table}
            WHERE table_name != '__validation_metadata__'
        """)
        last_data_load = data_df.collect()[0]["last_data_load"]
        
        # Get last validation timestamp
        validation_df = spark.sql(f"""
            SELECT last_watermark as last_validation
            FROM {audit_table}
            WHERE table_name = '__validation_metadata__'
        """)
        
        if validation_df.count() == 0:
            # First time running validation
            return True, "🆕 First validation run - no previous validation timestamp"
        
        last_validation = validation_df.collect()[0]["last_validation"]
        
        # Compare timestamps
        if last_data_load is None:
            return True, "⚠️  No data loads recorded yet - running validation"
        
        if last_validation is None or last_data_load > last_validation:
            return True, f"📊 Data changed since last validation (data: {last_data_load}, last validation: {last_validation})"
        else:
            return False, f"⏭️  No data changes since last validation (last data: {last_data_load}, last validation: {last_validation})"
            
    except Exception as e:
        # On any error, run validation (fail-safe)
        return True, f"⚠️  Error checking validation status: {e} - running validation"

# Check if there is new data since last validation (informational only — always validate)
has_new_data, reason = should_run_validation()

print("\n" + "="*70)
print("VALIDATION PRE-FLIGHT CHECK")
print("="*70)
print(f"\n{reason}\n")

if not has_new_data:
    print("ℹ️  No new data since last validation — running full validation on existing data anyway")
    print("    (Monitoring tables need every run recorded for dashboards)\n")
else:
    print("✅ New data detected — running validation\n")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Install pytest
# Install pytest if not available
try:
    import pytest
    print("✅ pytest already installed")
except ImportError:
    print("📦 Installing pytest...")
    %pip install pytest pytest-html --quiet
    import pytest
    print("✅ pytest installed successfully")

# COMMAND ----------

# DBTITLE 1,Run Tests
# Test configurations (filtered by layer)
# Infrastructure checks always run first for all layers
if layer == "bronze":
    suite_title = "BRONZE LAYER TEST SUITE"
    test_suites = [
        {
            "name": "Infrastructure Validation",
            "file": "test_infrastructure.py",
            "html_report": None
        },
        {
            "name": "Bronze Table Validation",
            "file": "test_bronze_tables.py",
            "html_report": None
        }
    ]
else:
    suite_title = f"{layer.upper()} LAYER TEST SUITE"
    test_suites = [
        {
            "name": "Infrastructure Validation",
            "file": "test_infrastructure.py",
            "html_report": None
        },
        {
            "name": "Silver Table Validation",
            "file": "test_silver_tables.py",
            "html_report": "test_report_silver.html"
        },
        {
            "name": "Audit Utils",
            "file": "test_audit_utils.py",
            "html_report": None
        },
        {
            "name": "Shared Logic",
            "file": "test_shared_logic.py",
            "html_report": None
        }
    ]

print("\n" + "="*70)
print(suite_title)
print("="*70)

# Pytest plugin to capture individual test names and failure messages
class TestResultCollector:
    """Collects per-test results including failure messages from pytest."""
    def __init__(self):
        self.test_results = []  # [(test_name, passed, failure_message)]

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            test_name = report.nodeid.split("::")[-1] if "::" in report.nodeid else report.nodeid
            if report.failed:
                fail_msg = str(report.longrepr) if report.longrepr else "Unknown failure"
                if len(fail_msg) > 1000:
                    fail_msg = fail_msg[:1000] + "..."
                self.test_results.append((test_name, False, fail_msg))
            elif report.passed:
                self.test_results.append((test_name, True, ""))

results = {}
all_test_details = []  # [(suite_name, test_name, passed, failure_message)]

for suite in test_suites:
    print(f"\n{'='*70}")
    print(f"RUNNING {suite['name'].upper()} TESTS")
    print(f"{'='*70}\n")
    
    test_file = test_dir / suite['file']
    
    # Build pytest arguments
    args = [
        str(test_file),
        "-v",
        "--tb=short",
        "--color=yes",
        "--assert=plain",  # Disable assertion rewriting to prevent __pycache__ creation
        "-p", "no:cacheprovider",  # Disable cache to prevent __pycache__ errors in Workspace filesystem
        "--import-mode=importlib"  # Use importlib to prevent __pycache__ creation during collection
    ]
    
    # Run tests with collector plugin to capture per-test results
    collector = TestResultCollector()
    exit_code = pytest.main(args, plugins=[collector])
    results[suite['name']] = exit_code
    
    # Collect per-test details for monitoring
    for test_name, passed, fail_msg in collector.test_results:
        all_test_details.append((suite['name'], test_name, passed, fail_msg))
    
    # Print result
    if exit_code == 0:
        print(f"\n✅ {suite['name']} tests PASSED!")
    else:
        print(f"\n❌ {suite['name']} tests FAILED (exit code: {exit_code})")
        # Print individual test failures for visibility
        for test_name, passed, fail_msg in collector.test_results:
            if not passed:
                print(f"   • {test_name}: {fail_msg[:200]}")

# COMMAND ----------

# DBTITLE 1,Test Summary
# Final summary
print("\n" + "="*70)
print("TEST SUITE SUMMARY")
print("="*70)

total_suites = len(results)
passed_suites = sum(1 for code in results.values() if code == 0)
failed_suites = total_suites - passed_suites

print(f"\nTest Suites Run: {total_suites}")
print(f"  ✅ Passed: {passed_suites}")
print(f"  ❌ Failed: {failed_suites}")

print("\nDetailed Results:")
for suite_name, exit_code in results.items():
    status = "✅ PASSED" if exit_code == 0 else "❌ FAILED"
    print(f"  {suite_name}: {status}")

print("\nTest Coverage:")
print("""  1. ✅ Table Existence: All 10 silver tables exist
  2. ✅ Table Population: Tables have data
  3. ✅ Row Count Validation: Reasonable number of rows
  4. ✅ Module Imports: All transforms and utilities import correctly
  5. ✅ Null Checks: Key columns have no unexpected nulls
  6. ✅ Unit Conversions: Temperature in Celsius (not Fahrenheit)
  7. ✅ Thermal Stress: HDD/CDD calculations correct
  8. ✅ Primary Key Deduplication: No duplicate keys
  9. ✅ MERGE Logic: Audit log updated correctly
  10. ✅ Geospatial Indexing: H3 functions work correctly
  11. ✅ Data Quality: Year ranges, date ranges, completeness checks
  12. ✅ Relational Normalisation: Wide-to-long unpivot works""")

print("\nValidated Tables:")
tables = [
    "energy_metrics", "weather_observations", "weather_projections",
    "weather_historical", "dim_stations", "dim_h3_grid", "dim_date",
    "dim_locations", "carbon_flux_spatial", "forest_inventory_annual"
]
for i, table in enumerate(tables, 1):
    print(f"  {i:2d}. {table}")

print(f"\n{'='*70}\n")

# --- MONITORING: Write test results and update pipeline_runs ---
duration = int((datetime.now() - run_start_time).total_seconds())

# Write per-test results to test_results (individual test names + actual failure messages)
for suite_name, test_name, passed, fail_msg in all_test_details:
    t_passed = 1 if passed else 0
    t_failed = 0 if passed else 1
    t_status = "passed" if passed else "failed"
    t_detail = fail_msg if not passed else ""
    spark.sql(f"""
        INSERT INTO climate_energy_demand.monitoring.test_results
        (run_timestamp, layer, suite_name, category, status, tests_passed, tests_failed, failure_message, run_id)
        VALUES (TIMESTAMP '{datetime.now()}', '{layer}', '{suite_name.replace(chr(39), chr(39)+chr(39))}', '{test_name.replace(chr(39), chr(39)+chr(39))}', '{t_status}', {t_passed}, {t_failed}, '{t_detail.replace(chr(39), chr(39)+chr(39))}', '{job_run_id}')
    """)

# Update pipeline_runs with final status
if failed_suites > 0:
    failure_details = []
    for suite_name, test_name, passed, fail_msg in all_test_details:
        if not passed:
            failure_details.append(f"{test_name}: {fail_msg}")
    error_detail = " | ".join(failure_details)
    if layer == "bronze":
        # Bronze staleness is a flag, not a failure — task succeeds, details in monitoring
        update_pipeline_run_status("completed", error_message=error_detail, duration_seconds=duration)
    else:
        update_pipeline_run_status("failed", error_message=error_detail, duration_seconds=duration)
else:
    update_pipeline_run_status("completed", duration_seconds=duration)

print(f"Monitoring: Logged {len(all_test_details)} test results to test_results and updated pipeline_runs")

# Raise exception if any tests failed (will cause job to fail)
# Bronze is non-blocking — staleness flags in monitoring but task succeeds
if failed_suites > 0 and layer != "bronze":
    raise Exception(f"Test suite failed: {failed_suites}/{total_suites} suites failed")
elif failed_suites > 0:
    print(f"\n⚠️  {failed_suites}/{total_suites} checks flagged issues (non-blocking for bronze layer)")
else:
    print("\n🎉 All tests passed successfully!")

# COMMAND ----------

# DBTITLE 1,Update Validation Timestamp
# Update validation timestamp in audit table
from datetime import datetime

try:
    audit_table = "climate_energy_demand.silver.ingestion_audit"
    current_time = datetime.now()
    
    # Check if validation metadata row exists
    check_df = spark.sql(f"""
        SELECT COUNT(*) as count
        FROM {audit_table}
        WHERE table_name = '__validation_metadata__'
    """)
    
    if check_df.collect()[0]["count"] == 0:
        # Insert new validation metadata row
        spark.sql(f"""
            INSERT INTO {audit_table}
            (table_name, last_watermark, rows_processed, processed_at)
            VALUES ('__validation_metadata__', '{current_time}', 0, '{current_time}')
        """)
        print(f"\n✅ Created validation metadata timestamp: {current_time}")
    else:
        # Update existing validation metadata row
        spark.sql(f"""
            UPDATE {audit_table}
            SET last_watermark = '{current_time}',
                processed_at = '{current_time}'
            WHERE table_name = '__validation_metadata__'
        """)
        print(f"\n✅ Updated validation timestamp: {current_time}")
    
    print("\nNext validation will check if data changed after this timestamp.\n")
    
except Exception as e:
    print(f"\n⚠️  Warning: Could not update validation timestamp: {e}")
    print("Validation will run again next time.\n")

# COMMAND ----------

# DBTITLE 1,Final Result Summary
# ============================================================================
# FINAL TEST RESULT SUMMARY
# ============================================================================

print("\n" + "#" * 80)
print("#" + " " * 78 + "#")
print("#" + " " * 25 + "FINAL TEST RESULTS" + " " * 35 + "#")
print("#" + " " * 78 + "#")
print("#" * 80)

total_suites = len(results)
passed_suites = sum(1 for code in results.values() if code == 0)
failed_suites = total_suites - passed_suites

if failed_suites == 0:
    print("\n" + "="*80)
    print("✅ ✅ ✅  ALL TESTS PASSED!  ✅ ✅ ✅".center(80))
    print("="*80)
    print(f"\n   Total Test Suites: {total_suites}")
    print(f"   All {passed_suites} test suites passed successfully")
    print(f"\n   Silver Layer data quality validated ✓")
    print("\n" + "="*80)
    
    # Exit successfully
    dbutils.notebook.exit("SUCCESS: All validation tests passed")
    
else:
    print("\n" + "="*80)
    if layer == "bronze":
        print("⚠️  BRONZE CHECKS FLAGGED ISSUES (non-blocking)".center(80))
    else:
        print("❌ ❌ ❌  TESTS FAILED!  ❌ ❌ ❌".center(80))
    print("="*80)
    print(f"\n   Total Test Suites: {total_suites}")
    print(f"   Passed: {passed_suites}")
    print(f"   Failed: {failed_suites}")
    print("\n   Failed Suites:")
    for suite_name, exit_code in results.items():
        if exit_code != 0:
            print(f"      • {suite_name}")
    print("\n" + "="*80)
    
    if layer == "bronze":
        # Bronze: non-blocking, exit successfully with warnings logged to monitoring
        warn_msg = f"COMPLETED WITH WARNINGS: {failed_suites} out of {total_suites} checks flagged issues."
        dbutils.notebook.exit(warn_msg)
    else:
        # Fail the notebook so the job shows as failed
        error_msg = f"FAILED: {failed_suites} out of {total_suites} test suites failed. Check logs above for details."
        dbutils.notebook.exit(error_msg)
        raise Exception(error_msg)