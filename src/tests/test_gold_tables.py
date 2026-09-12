# Location: src/tests/test_gold_tables.py
# Purpose: Comprehensive validation of Gold Layer tables

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, max as spark_max, min as spark_min, avg as spark_avg
from datetime import datetime
import sys
import os

# Import project modules for validation
from src.common.audit_utils import get_last_watermark

# =============================================================================
# FIXTURES
# =============================================================================

# Spark fixture is now provided by conftest.py

# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

# All gold tables that should exist
EXPECTED_GOLD_TABLES = [
    "climate_energy_demand.gold.fct_energy_demand_daily",
    "climate_energy_demand.gold.fct_forest_resilience_annual",
    "climate_energy_demand.gold.fct_ground_truth_verification_daily"
]

# Key columns that should not be null for each table
KEY_COLUMNS = {
    "climate_energy_demand.gold.fct_energy_demand_daily": ["iso_code", "date"],
    "climate_energy_demand.gold.fct_forest_resilience_annual": ["h3_cell_index", "year"],
    "climate_energy_demand.gold.fct_ground_truth_verification_daily": ["station_id", "date"]
}

# Expected minimum row counts (adjust based on your data)
MIN_ROW_COUNTS = {
    "climate_energy_demand.gold.fct_energy_demand_daily": 1000,
    "climate_energy_demand.gold.fct_forest_resilience_annual": 100,
    "climate_energy_demand.gold.fct_ground_truth_verification_daily": 1000
}

# =============================================================================
# TEST 1: TABLE EXISTENCE AND POPULATION
# =============================================================================

class TestTableExistence:
    """Test that all gold tables exist and have data."""
    
    def test_audit_table_exists(self, spark):
        """Verify the gold audit table exists."""
        audit_table = "climate_energy_demand.gold.ingestion_audit"
        assert spark.catalog.tableExists(audit_table), \
            f"Gold audit table {audit_table} does not exist"
    
    @pytest.mark.parametrize("table_name", EXPECTED_GOLD_TABLES)
    def test_table_exists(self, spark, table_name):
        """Test that each gold table exists."""
        assert spark.catalog.tableExists(table_name), \
            f"Gold table {table_name} does not exist"
    
    @pytest.mark.parametrize("table_name", EXPECTED_GOLD_TABLES)
    def test_table_has_data(self, spark, table_name):
        """Test that each gold table has data."""
        df = spark.table(table_name)
        row_count = df.count()
        assert row_count > 0, \
            f"Gold table {table_name} exists but is empty"
    
    @pytest.mark.parametrize("table_name", EXPECTED_GOLD_TABLES)
    def test_table_row_count_sane(self, spark, table_name):
        """Test that each table has a reasonable number of rows."""
        df = spark.table(table_name)
        row_count = df.count()
        min_expected = MIN_ROW_COUNTS.get(table_name, 1)
        assert row_count >= min_expected, \
            f"Table {table_name} has {row_count} rows, expected at least {min_expected}"

# =============================================================================
# TEST 2: MODULE IMPORTS
# =============================================================================

class TestImports:
    """Test that all gold transform modules can be imported."""
    
    def test_import_gold_transforms(self):
        """Test all gold transform functions import correctly."""
        from src.transforms import energy, nature, quality
        
        # Check energy module has gold function
        assert hasattr(energy, 'process_energy_demand'), \
            "energy module missing process_energy_demand function"
        
        # Check nature module has gold function
        assert hasattr(nature, 'process_forest_resilience'), \
            "nature module missing process_forest_resilience function"
        
        # Check quality module exists and has gold function
        assert hasattr(quality, 'process_fidelity_audit'), \
            "quality module missing process_fidelity_audit function"

# =============================================================================
# TEST 3: NULL VALUE CHECKS
# =============================================================================

class TestNullValues:
    """Test that key columns don't have unexpected nulls."""
    
    @pytest.mark.parametrize("table_name", EXPECTED_GOLD_TABLES)
    def test_no_nulls_in_key_columns(self, spark, table_name):
        """Test that key columns (PKs) have no null values."""
        if table_name not in KEY_COLUMNS:
            pytest.skip(f"No key columns defined for {table_name}")
        
        df = spark.table(table_name)
        key_cols = KEY_COLUMNS[table_name]
        
        for key_col in key_cols:
            if key_col in df.columns:
                null_count = df.filter(col(key_col).isNull()).count()
                assert null_count == 0, \
                    f"Table {table_name} has {null_count} null values in key column {key_col}"

# =============================================================================
# TEST 4: BUSINESS LOGIC VALIDATION
# =============================================================================

class TestBusinessLogic:
    """Test business logic and calculated fields."""
    
    def test_energy_demand_sensitivity_index(self, spark):
        """Test demand_sensitivity_index is calculated and reasonable."""
        table_name = "climate_energy_demand.gold.fct_energy_demand_daily"
        
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        if "demand_sensitivity_index" not in df.columns:
            pytest.skip("demand_sensitivity_index column not found")
        
        # Check that DSI is non-negative and not null where baseline exists
        invalid_count = df.filter(
            (col("annual_baseline_twh").isNotNull()) &
            (col("demand_sensitivity_index").isNull() | (col("demand_sensitivity_index") < 0))
        ).count()
        
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with invalid demand_sensitivity_index"
    
    def test_temperature_anomaly_range(self, spark):
        """Test temp_anomaly values are in reasonable range."""
        table_name = "climate_energy_demand.gold.fct_energy_demand_daily"
        
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        if "temp_anomaly" not in df.columns:
            pytest.skip("temp_anomaly column not found")
        
        # Anomalies beyond ±30°C from baseline are extremely rare
        stats = df.filter(col("temp_anomaly").isNotNull()).select(
            spark_min("temp_anomaly").alias("min_anomaly"),
            spark_max("temp_anomaly").alias("max_anomaly")
        ).collect()[0]
        
        assert stats["min_anomaly"] >= -30, \
            f"Minimum temp_anomaly {stats['min_anomaly']} is suspiciously low"
        assert stats["max_anomaly"] <= 30, \
            f"Maximum temp_anomaly {stats['max_anomaly']} is suspiciously high"
    
    def test_forest_sequestration_efficiency(self, spark):
        """Test sequestration_efficiency is calculated correctly."""
        table_name = "climate_energy_demand.gold.fct_forest_resilience_annual"
        
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        if "sequestration_efficiency" not in df.columns:
            pytest.skip("sequestration_efficiency column not found")
        
        # Where forest_area_ha > 0, sequestration_efficiency should not be null
        invalid_count = df.filter(
            (col("forest_area_ha") > 0) &
            col("sequestration_efficiency").isNull()
        ).count()
        
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with missing sequestration_efficiency despite having forest area"
    
    def test_ground_truth_error_metrics(self, spark):
        """Test that error metrics are calculated correctly."""
        table_name = "climate_energy_demand.gold.fct_ground_truth_verification_daily"
        
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        # Check absolute_error = |modeled - observed|
        if all(c in df.columns for c in ["absolute_error", "bias_error"]):
            # Absolute error should always be >= 0
            negative_errors = df.filter(col("absolute_error") < 0).count()
            assert negative_errors == 0, \
                f"Found {negative_errors} rows with negative absolute_error"
            
            # Absolute error should equal |bias_error|
            # Allow for floating point tolerance
            tolerance = 0.01
            invalid_errors = df.filter(
                (col("absolute_error").isNotNull()) &
                (col("bias_error").isNotNull()) &
                (abs(col("absolute_error") - abs(col("bias_error"))) > tolerance)
            ).count()
            
            assert invalid_errors == 0, \
                f"Found {invalid_errors} rows where absolute_error != |bias_error|"
        
        # Check is_within_tolerance flag
        if "is_within_tolerance" in df.columns:
            # When absolute_error <= 2.0, flag should be True
            mismatched = df.filter(
                (col("absolute_error").isNotNull()) &
                (col("absolute_error") <= 2.0) &
                (col("is_within_tolerance") == False)
            ).count()
            
            assert mismatched == 0, \
                f"Found {mismatched} rows where error <= 2.0 but is_within_tolerance is False"

# =============================================================================
# TEST 5: DATA QUALITY CHECKS
# =============================================================================

class TestDataQuality:
    """Test overall data quality metrics."""
    
    @pytest.mark.parametrize("table_name", EXPECTED_GOLD_TABLES)
    def test_no_duplicate_primary_keys(self, spark, table_name):
        """Test that tables have no duplicate primary key combinations."""
        if table_name not in KEY_COLUMNS:
            pytest.skip(f"No key columns defined for {table_name}")
        
        df = spark.table(table_name)
        key_cols = KEY_COLUMNS[table_name]
        
        # Check if all key columns exist
        missing_cols = [k for k in key_cols if k not in df.columns]
        if missing_cols:
            pytest.skip(f"Missing key columns: {missing_cols}")
        
        total_count = df.count()
        distinct_count = df.select(key_cols).distinct().count()
        
        assert total_count == distinct_count, \
            f"Table {table_name} has {total_count - distinct_count} duplicate primary keys"
    
    def test_date_ranges_reasonable(self, spark):
        """Test that date columns are in reasonable ranges."""
        date_tables = [
            "climate_energy_demand.gold.fct_energy_demand_daily",
            "climate_energy_demand.gold.fct_ground_truth_verification_daily"
        ]
        
        for table_name in date_tables:
            if not spark.catalog.tableExists(table_name):
                continue
            
            df = spark.table(table_name)
            
            if "date" not in df.columns:
                continue
            
            # Check dates are between 2010 and 2040 (our defined range)
            out_of_range = df.filter(
                (col("date") < "2010-01-01") |
                (col("date") > "2040-12-31")
            ).count()
            
            assert out_of_range == 0, \
                f"Table {table_name} has {out_of_range} rows with dates outside 2010-2040 range"

# =============================================================================
# TEST 6: REFERENTIAL INTEGRITY
# =============================================================================

class TestReferentialIntegrity:
    """Test relationships between gold and silver tables."""
    
    def test_energy_demand_references_silver(self, spark):
        """Test that energy demand table data exists in silver sources."""
        gold_table = "climate_energy_demand.gold.fct_energy_demand_daily"
        silver_weather = "climate_energy_demand.silver.weather_historical"
        silver_energy = "climate_energy_demand.silver.energy_metrics"
        
        if not all(spark.catalog.tableExists(t) for t in [gold_table, silver_weather, silver_energy]):
            pytest.skip("Required tables don't exist")
        
        gold_df = spark.table(gold_table)
        
        # Check that iso_codes in gold exist in silver energy
        if "iso_code" in gold_df.columns:
            gold_countries = gold_df.select("iso_code").distinct()
            silver_df = spark.table(silver_energy)
            
            if "iso_code" in silver_df.columns:
                silver_countries = silver_df.select("iso_code").distinct()
                
                # Gold countries should be subset of silver countries
                orphaned = gold_countries.join(silver_countries, on="iso_code", how="left_anti")
                orphan_count = orphaned.count()
                
                assert orphan_count == 0, \
                    f"Found {orphan_count} iso_codes in gold that don't exist in silver energy metrics"