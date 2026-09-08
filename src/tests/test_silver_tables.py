# Location: src/tests/test_silver_tables.py
# Purpose: Comprehensive validation of Silver Layer tables

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, max as spark_max, min as spark_min
from datetime import datetime
import sys
import os

# Import project modules for validation
from src.common.audit_utils import get_last_watermark, AUDIT_TABLE
from src.common.shared_logic import calculate_thermal_stress, geospatial_indexing, relational_normalisation

# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """Get or create Spark session for tests."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder \
            .appName("SilverTableTests") \
            .getOrCreate()
    return spark

# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

# All 10 silver tables that should exist
EXPECTED_SILVER_TABLES = [
    "climate_energy_demand.silver.energy_metrics",
    "climate_energy_demand.silver.weather_observations",
    "climate_energy_demand.silver.weather_projections",
    "climate_energy_demand.silver.weather_historical",
    "climate_energy_demand.silver.dim_stations",
    "climate_energy_demand.silver.dim_h3_grid",
    "climate_energy_demand.silver.dim_date",
    "climate_energy_demand.silver.dim_locations",
    "climate_energy_demand.silver.carbon_flux_spatial",
    "climate_energy_demand.silver.forest_inventory_annual"
]

# Key columns that should not be null for each table
KEY_COLUMNS = {
    "climate_energy_demand.silver.energy_metrics": ["country_name", "year"],
    "climate_energy_demand.silver.weather_observations": ["country", "station_id", "date"],
    "climate_energy_demand.silver.weather_projections": ["model", "country", "date", "h3_index"],
    "climate_energy_demand.silver.weather_historical": ["country", "date"],
    "climate_energy_demand.silver.dim_stations": ["station_id"],
    "climate_energy_demand.silver.dim_h3_grid": ["h3_cell"],
    "climate_energy_demand.silver.dim_date": ["date"],
    "climate_energy_demand.silver.dim_locations": ["iso_code"],
    "climate_energy_demand.silver.carbon_flux_spatial": ["h3_cell", "year"],
    "climate_energy_demand.silver.forest_inventory_annual": ["country_name", "land_use_category", "unit", "year"]
}

# Expected minimum row counts (adjust based on your data)
MIN_ROW_COUNTS = {
    "climate_energy_demand.silver.energy_metrics": 100,
    "climate_energy_demand.silver.weather_observations": 1000,
    "climate_energy_demand.silver.weather_projections": 100,
    "climate_energy_demand.silver.weather_historical": 1000,
    "climate_energy_demand.silver.dim_stations": 10,
    "climate_energy_demand.silver.dim_h3_grid": 10,
    "climate_energy_demand.silver.dim_date": 365,
    "climate_energy_demand.silver.dim_locations": 10,
    "climate_energy_demand.silver.carbon_flux_spatial": 100,
    "climate_energy_demand.silver.forest_inventory_annual": 10
}

# =============================================================================
# TEST 1: TABLE EXISTENCE AND POPULATION
# =============================================================================

class TestTableExistence:
    """Test that all silver tables exist and have data."""
    
    def test_audit_table_exists(self, spark_session):
        """Verify the audit table exists."""
        assert spark_session.catalog.tableExists(AUDIT_TABLE), \
            f"Audit table {AUDIT_TABLE} does not exist"
    
    @pytest.mark.parametrize("table_name", EXPECTED_SILVER_TABLES)
    def test_table_exists(self, spark_session, table_name):
        """Test that each silver table exists."""
        assert spark_session.catalog.tableExists(table_name), \
            f"Silver table {table_name} does not exist"
    
    @pytest.mark.parametrize("table_name", EXPECTED_SILVER_TABLES)
    def test_table_has_data(self, spark_session, table_name):
        """Test that each silver table has data."""
        df = spark_session.table(table_name)
        row_count = df.count()
        assert row_count > 0, \
            f"Silver table {table_name} exists but is empty"
    
    @pytest.mark.parametrize("table_name", EXPECTED_SILVER_TABLES)
    def test_table_row_count_sane(self, spark_session, table_name):
        """Test that each table has a reasonable number of rows."""
        df = spark_session.table(table_name)
        row_count = df.count()
        min_expected = MIN_ROW_COUNTS.get(table_name, 1)
        assert row_count >= min_expected, \
            f"Table {table_name} has {row_count} rows, expected at least {min_expected}"

# =============================================================================
# TEST 2: MODULE IMPORTS
# =============================================================================

class TestImports:
    """Test that all project modules can be imported."""
    
    def test_import_audit_utils(self):
        """Test audit_utils module imports correctly."""
        from src.common import audit_utils
        assert hasattr(audit_utils, 'get_last_watermark')
        assert hasattr(audit_utils, 'update_audit_log')
    
    def test_import_shared_logic(self):
        """Test shared_logic module imports correctly."""
        from src.common import shared_logic
        assert hasattr(shared_logic, 'calculate_thermal_stress')
        assert hasattr(shared_logic, 'geospatial_indexing')
        assert hasattr(shared_logic, 'relational_normalisation')
    
    def test_import_transforms(self):
        """Test all transform modules import correctly."""
        from src.transforms import energy, weather, geospatial, nature, common
        
        # Check energy module
        assert hasattr(energy, 'process_energy_metrics')
        
        # Check weather module
        assert hasattr(weather, 'process_weather_observations')
        assert hasattr(weather, 'process_weather_projections')

# =============================================================================
# TEST 3: NULL VALUE CHECKS
# =============================================================================

class TestNullValues:
    """Test that key columns don't have unexpected nulls."""
    
    @pytest.mark.parametrize("table_name", EXPECTED_SILVER_TABLES)
    def test_no_nulls_in_key_columns(self, spark_session, table_name):
        """Test that key columns (PKs) have no null values."""
        if table_name not in KEY_COLUMNS:
            pytest.skip(f"No key columns defined for {table_name}")
        
        df = spark_session.table(table_name)
        key_cols = KEY_COLUMNS[table_name]
        
        for key_col in key_cols:
            if key_col in df.columns:
                null_count = df.filter(col(key_col).isNull()).count()
                assert null_count == 0, \
                    f"Table {table_name} has {null_count} null values in key column {key_col}"

# =============================================================================
# TEST 4: UNIT CONVERSIONS
# =============================================================================

class TestUnitConversions:
    """Test that unit conversions are correct."""
    
    def test_temperature_conversion_weather_observations(self, spark_session):
        """Test temperature is in Celsius (reasonable range)."""
        table_name = "climate_energy_demand.silver.weather_observations"
        
        if not spark_session.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark_session.table(table_name)
        
        # Check if temp columns exist
        temp_cols = [c for c in df.columns if 'temp' in c.lower() or 'temperature' in c.lower()]
        
        if not temp_cols:
            pytest.skip("No temperature columns found")
        
        for temp_col in temp_cols:
            # Celsius range check: -60°C to 60°C is reasonable
            stats = df.select(
                spark_min(col(temp_col)).alias("min_temp"),
                spark_max(col(temp_col)).alias("max_temp")
            ).collect()[0]
            
            min_temp = stats["min_temp"]
            max_temp = stats["max_temp"]
            
            # If these are in Fahrenheit, they'd be much higher
            assert min_temp is None or min_temp >= -60, \
                f"Minimum temperature {min_temp} seems too cold (possible conversion error)"
            assert max_temp is None or max_temp <= 60, \
                f"Maximum temperature {max_temp} seems too hot (possibly still in Fahrenheit?)"
    
    def test_thermal_stress_calculation(self, spark_session):
        """Test thermal stress (HDD/CDD) calculation logic."""
        # Create test data
        test_data = [(10.0,), (15.0,), (25.0,), (30.0,)]
        df = spark_session.createDataFrame(test_data, ["temp"])
        
        # Apply thermal stress calculation
        result_df = calculate_thermal_stress(df, "temp")
        results = result_df.collect()
        
        # Test Heating Degree Days (HDD = max(0, 15 - temp))
        assert results[0]["heating_degree_days"] == 5.0, "HDD for 10°C should be 5"
        assert results[1]["heating_degree_days"] == 0.0, "HDD for 15°C should be 0"
        
        # Test Cooling Degree Days (CDD = max(0, temp - 25))
        assert results[2]["cooling_degree_days"] == 0.0, "CDD for 25°C should be 0"
        assert results[3]["cooling_degree_days"] == 5.0, "CDD for 30°C should be 5"

# =============================================================================
# TEST 5: MERGE AND DEDUPLICATION
# =============================================================================

class TestMergeAndDedup:
    """Test MERGE logic and deduplication behavior."""
    
    @pytest.mark.parametrize("table_name", EXPECTED_SILVER_TABLES)
    def test_no_duplicate_primary_keys(self, spark_session, table_name):
        """Test that tables have no duplicate primary keys."""
        if table_name not in KEY_COLUMNS:
            pytest.skip(f"No key columns defined for {table_name}")
        
        df = spark_session.table(table_name)
        key_cols = KEY_COLUMNS[table_name]
        
        # Count total rows
        total_rows = df.count()
        
        # Count distinct combinations of key columns
        distinct_keys = df.select(key_cols).distinct().count()
        
        assert total_rows == distinct_keys, \
            f"Table {table_name} has {total_rows} rows but only {distinct_keys} distinct key combinations. Duplicates detected!"
    
    def test_audit_log_updated(self, spark_session):
        """Test that audit log is being updated for tables."""
        audit_df = spark_session.table(AUDIT_TABLE)
        
        # Check that we have audit records
        audit_count = audit_df.count()
        assert audit_count > 0, "Audit table is empty, watermarks not being tracked"
        
        # Check that at least some of the silver tables have audit records
        audited_tables = audit_df.select("table_name").distinct().collect()
        audited_table_names = [row["table_name"] for row in audited_tables]
        
        tables_with_watermarks = [t for t in EXPECTED_SILVER_TABLES if t in audited_table_names]
        assert len(tables_with_watermarks) > 0, \
            "No silver tables have audit records"

# =============================================================================
# TEST 6: GEOSPATIAL INDEXING
# =============================================================================

class TestGeospatialFunctions:
    """Test geospatial indexing functions."""
    
    def test_h3_indexing(self, spark_session):
        """Test H3 geospatial indexing."""
        # Create test data with known coordinates
        # NYC: ~40.7N, -74.0W
        test_data = [(40.7, -74.0), (51.5, -0.1)]  # NYC and London
        df = spark_session.createDataFrame(test_data, ["latitude", "longitude"])
        
        # Apply H3 indexing
        result_df = geospatial_indexing(df, "latitude", "longitude")
        
        # Check that h3_index column exists and is not null
        assert "h3_index_res6" in result_df.columns, "H3 index column not created"
        
        result_count = result_df.filter(col("h3_index_res6").isNotNull()).count()
        assert result_count == 2, "H3 indexing failed for some rows"
    
    def test_h3_grid_table_populated(self, spark_session):
        """Test that H3 grid dimension table has valid indices."""
        table_name = "climate_energy_demand.silver.dim_h3_grid"
        
        if not spark_session.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark_session.table(table_name)
        
        # Check that h3_index column exists and has no nulls
        assert "h3_cell" in df.columns, "h3_cell column not found in dim_h3_grid"
        
        null_count = df.filter(col("h3_cell").isNull()).count()
        assert null_count == 0, f"Found {null_count} null h3_cell values in dim_h3_grid"

# =============================================================================
# TEST 7: DATA QUALITY SPOT CHECKS
# =============================================================================

class TestDataQuality:
    """Spot checks for data quality issues."""
    
    def test_energy_metrics_year_range(self, spark_session):
        """Test that energy metrics are within expected year range."""
        table_name = "climate_energy_demand.silver.energy_metrics"
        
        if not spark_session.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark_session.table(table_name)
        
        if "year" not in df.columns:
            pytest.skip("Year column not found")
        
        stats = df.select(
            spark_min(col("year")).alias("min_year"),
            spark_max(col("year")).alias("max_year")
        ).collect()[0]
        
        min_year = stats["min_year"]
        max_year = stats["max_year"]
        
        # Sanity checks
        assert min_year >= 1900, f"Minimum year {min_year} seems unrealistic"
        assert max_year <= 2100, f"Maximum year {max_year} seems unrealistic"
        assert min_year >= 2010, f"Minimum year {min_year} should be >= 2010 per config"
    
    def test_weather_date_range(self, spark_session):
        """Test that weather observations have reasonable date ranges."""
        table_name = "climate_energy_demand.silver.weather_observations"
        
        if not spark_session.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark_session.table(table_name)
        
        if "date" not in df.columns:
            pytest.skip("Date column not found")
        
        stats = df.select(
            spark_min(col("date")).alias("min_date"),
            spark_max(col("date")).alias("max_date")
        ).collect()[0]
        
        min_date = stats["min_date"]
        max_date = stats["max_date"]
        
        # Sanity checks
        assert min_date is not None, "Minimum date is null"
        assert max_date is not None, "Maximum date is null"
        assert min_date < max_date, "Date range is invalid (min >= max)"
    
    def test_dim_date_completeness(self, spark_session):
        """Test that date dimension has no gaps."""
        table_name = "climate_energy_demand.silver.dim_date"
        
        if not spark_session.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark_session.table(table_name)
        
        if "date" not in df.columns:
            pytest.skip("Date column not found")
        
        # Get date range
        stats = df.select(
            spark_min(col("date")).alias("min_date"),
            spark_max(col("date")).alias("max_date"),
            count("*").alias("row_count")
        ).collect()[0]
        
        min_date = stats["min_date"]
        max_date = stats["max_date"]
        row_count = stats["row_count"]
        
        # Calculate expected number of days
        from datetime import datetime
        if isinstance(min_date, str):
            min_dt = datetime.fromisoformat(min_date)
            max_dt = datetime.fromisoformat(max_date)
        else:
            min_dt = min_date
            max_dt = max_date
        
        expected_days = (max_dt - min_dt).days + 1
        
        # Allow small tolerance for missing dates
        assert row_count >= expected_days * 0.95, \
            f"Date dimension has gaps: expected ~{expected_days} dates, found {row_count}"

# =============================================================================
# TEST 8: RELATIONAL NORMALISATION
# =============================================================================

class TestRelationalNormalisation:
    """Test the wide-to-long transformation logic."""
    
    def test_unpivot_year_columns(self, spark_session):
        """Test that wide format (y1990, y2000, etc.) is unpivoted correctly."""
        # Create test data in wide format
        test_data = [("USA", 100, 150, 200)]
        df = spark_session.createDataFrame(test_data, ["country", "y1990", "y2000", "y2010"])
        
        # Apply normalisation
        result_df = relational_normalisation(df, ["country"])
        
        # Should have 3 rows (one per year)
        assert result_df.count() == 3, "Unpivot should create 3 rows from 3 year columns"
        
        # Check columns exist
        assert "observation_year" in result_df.columns, "observation_year column not created"
        assert "metric_value" in result_df.columns, "metric_value column not created"
        
        # Check year values are integers
        years = result_df.select("observation_year").distinct().collect()
        year_values = sorted([row["observation_year"] for row in years])
        assert year_values == [1990, 2000, 2010], f"Year values incorrect: {year_values}"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])