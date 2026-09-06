# Location: src/tests/test_audit_utils.py
# Purpose: Test watermark and audit logging functionality

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
import sys
import os

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.common.audit_utils import get_last_watermark, update_audit_log, AUDIT_TABLE

@pytest.fixture(scope="session")
def spark_session():
    """Get or create Spark session for tests."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder \
            .appName("AuditUtilsTests") \
            .getOrCreate()
    return spark

def test_audit_table_exists(spark_session):
    """Test that the audit table exists."""
    assert spark_session.catalog.tableExists(AUDIT_TABLE), \
        f"Audit table {AUDIT_TABLE} should exist after setup"

def test_get_last_watermark_default(spark_session):
    """Test that get_last_watermark returns epoch for non-existent table."""
    # Use a table name that definitely doesn't exist in audit log
    fake_table = "climate_energy_demand.silver.fake_table_xyz"
    watermark = get_last_watermark(fake_table)
    
    # Should return epoch (1900-01-01)
    assert watermark.year == 1900, \
        f"Expected epoch year 1900 for non-existent table, got {watermark.year}"

def test_get_last_watermark_existing(spark_session):
    """Test that get_last_watermark retrieves actual watermarks."""
    # Check if audit table has any records
    audit_df = spark_session.table(AUDIT_TABLE)
    count = audit_df.count()
    
    if count == 0:
        pytest.skip("No audit records exist yet")
    
    # Get the first table name from audit log
    first_record = audit_df.first()
    table_name = first_record["table_name"]
    
    # Get watermark for that table
    watermark = get_last_watermark(table_name)
    
    # Should not be epoch
    assert watermark.year > 1900, \
        f"Watermark for {table_name} should be after epoch, got {watermark}"

def test_update_audit_log_creates_record(spark_session):
    """Test that update_audit_log creates a record."""
    test_table = "climate_energy_demand.silver.test_table"
    test_watermark = datetime(2024, 1, 1, 12, 0, 0)
    test_count = 1000
    
    # Get count before
    audit_df = spark_session.table(AUDIT_TABLE)
    count_before = audit_df.filter(audit_df.table_name == test_table).count()
    
    # Update audit log
    update_audit_log(test_table, test_watermark, test_count)
    
    # Get count after
    spark_session.catalog.refreshTable(AUDIT_TABLE)
    audit_df_after = spark_session.table(AUDIT_TABLE)
    count_after = audit_df_after.filter(audit_df_after.table_name == test_table).count()
    
    assert count_after > count_before, \
        "Audit log should have a new record after update_audit_log"

def test_audit_log_has_required_columns(spark_session):
    """Test that audit table has all required columns."""
    audit_df = spark_session.table(AUDIT_TABLE)
    
    required_columns = ["table_name", "last_watermark", "rows_processed", "processed_at"]
    
    for col in required_columns:
        assert col in audit_df.columns, \
            f"Audit table missing required column: {col}"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])