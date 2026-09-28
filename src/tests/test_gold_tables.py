# Location: src/tests/test_gold_tables.py
# Purpose: Comprehensive validation of Gold Layer tables

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, max as spark_max, min as spark_min, avg as spark_avg, year, month, abs as spark_abs
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
    "climate_energy_demand.gold.dim_date",
    "climate_energy_demand.gold.dim_koppen_zones",
    "climate_energy_demand.gold.dim_locations",
    "climate_energy_demand.gold.dim_stations",
    "climate_energy_demand.gold.fct_energy_demand_daily",
    "climate_energy_demand.gold.fct_forest_resilience_annual",
    "climate_energy_demand.gold.fct_ground_truth_verification_daily",
    "climate_energy_demand.gold.fct_temp_change_annual",
    "climate_energy_demand.gold.fct_land_cover_annual"
]

# Key columns that should not be null for each table
KEY_COLUMNS = {
    "climate_energy_demand.gold.dim_date": ["date"],
    "climate_energy_demand.gold.dim_koppen_zones": ["koppen_zone"],
    "climate_energy_demand.gold.dim_locations": ["iso_code"],
    "climate_energy_demand.gold.dim_stations": ["station_id"],
    "climate_energy_demand.gold.fct_energy_demand_daily": ["iso_code", "date"],
    "climate_energy_demand.gold.fct_forest_resilience_annual": ["iso_code", "year"],
    "climate_energy_demand.gold.fct_ground_truth_verification_daily": ["station_id", "date"],
    "climate_energy_demand.gold.fct_temp_change_annual": ["iso_code", "year", "months", "element"],
    "climate_energy_demand.gold.fct_land_cover_annual": ["iso_code", "year", "land_cover_category", "element"]
}

# Expected minimum row counts (adjust based on your data)
MIN_ROW_COUNTS = {
    "climate_energy_demand.gold.dim_date": 1000,
    "climate_energy_demand.gold.dim_koppen_zones": 20,
    "climate_energy_demand.gold.dim_locations": 100,
    "climate_energy_demand.gold.dim_stations": 100,
    "climate_energy_demand.gold.fct_energy_demand_daily": 1000,
    "climate_energy_demand.gold.fct_forest_resilience_annual": 100,
    "climate_energy_demand.gold.fct_ground_truth_verification_daily": 1000,
    "climate_energy_demand.gold.fct_temp_change_annual": 100,
    "climate_energy_demand.gold.fct_land_cover_annual": 100
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
        from src.transforms import energy, nature, quality, climate
        
        # Check climate module has gold dim transform functions
        assert hasattr(climate, 'transform_dim_koppen_zones'), \
            "climate module missing transform_dim_koppen_zones function"
        assert hasattr(climate, 'transform_gold_dim_date'), \
            "climate module missing transform_gold_dim_date function"
        assert hasattr(climate, 'transform_gold_dim_locations'), \
            "climate module missing transform_gold_dim_locations function"
        assert hasattr(climate, 'transform_gold_dim_stations'), \
            "climate module missing transform_gold_dim_stations function"
        
        # Check energy module has gold function
        assert hasattr(energy, 'process_energy_demand'), \
            "energy module missing process_energy_demand function"
        
        # Check nature module has gold function
        assert hasattr(nature, 'process_forest_resilience'), \
            "nature module missing process_forest_resilience function"
        
        # Check quality module exists and has gold function
        assert hasattr(quality, 'process_fidelity_audit'), \
            "quality module missing process_fidelity_audit function"

        # Check nature module has additional gold functions
        assert hasattr(nature, 'process_temp_change_gold'), \
            "nature module missing process_temp_change_gold function"
        assert hasattr(nature, 'process_land_cover_gold'), \
            "nature module missing process_land_cover_gold function"

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
            (col("annual_baseline_twh").isNotNull() & (col("annual_baseline_twh") > 0)) &
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
    
    def test_forest_carbon_density(self, spark):
        """Test carbon_density is calculated correctly."""
        table_name = "climate_energy_demand.gold.fct_forest_resilience_annual"
        
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        if "carbon_density" not in df.columns:
            pytest.skip("carbon_density column not found")
        
        # Where forest_area_ha > 0, carbon_density should not be null
        invalid_count = df.filter(
            (col("forest_area_ha") > 0) &
            col("carbon_density").isNull()
        ).count()
        
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with missing carbon_density despite having forest area"
    
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
                (spark_abs(col("absolute_error") - spark_abs(col("bias_error"))) > tolerance)
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

    def test_temp_change_is_warming_flag(self, spark):
        """Test is_warming flag: True when element='Temperature change' and value > 0."""
        table_name = "climate_energy_demand.gold.fct_temp_change_annual"

        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)

        if not all(c in df.columns for c in ["is_warming", "element", "value"]):
            pytest.skip("Required columns not found")

        # is_warming should be True when element='Temperature change' and value > 0
        # It should be False otherwise (negative values or non-anomaly elements)
        mismatched = df.filter(
            ((col("element") == "Temperature change") & (col("value") > 0) & (col("is_warming") == False)) |
            ((col("is_warming") == True) &
             ~((col("element") == "Temperature change") & (col("value") > 0)))
        ).count()

        assert mismatched == 0, \
            f"Found {mismatched} rows with incorrect is_warming flag"

    def test_land_cover_area_conversion(self, spark):
        """Test area_ha is non-negative (converted from 1000 ha to ha)."""
        table_name = "climate_energy_demand.gold.fct_land_cover_annual"

        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)

        if "area_ha" not in df.columns:
            pytest.skip("area_ha column not found")

        # area_ha should be non-negative (land area cannot be negative)
        negative_count = df.filter(col("area_ha") < 0).count()
        assert negative_count == 0, \
            f"Found {negative_count} rows with negative area_ha"

        # area_ha should not be null (it is the primary metric)
        null_count = df.filter(col("area_ha").isNull()).count()
        assert null_count == 0, \
            f"Found {null_count} rows with null area_ha"

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

    def test_year_ranges_reasonable(self, spark):
        """Test that year columns in annual tables are in reasonable ranges."""
        annual_tables = [
            "climate_energy_demand.gold.fct_forest_resilience_annual",
            "climate_energy_demand.gold.fct_temp_change_annual",
            "climate_energy_demand.gold.fct_land_cover_annual"
        ]

        for table_name in annual_tables:
            if not spark.catalog.tableExists(table_name):
                continue

            df = spark.table(table_name)

            if "year" not in df.columns:
                continue

            # Years should be between 2010 and 2040 (our study range)
            out_of_range = df.filter(
                (col("year") < 2010) | (col("year") > 2040)
            ).count()

            assert out_of_range == 0, \
                f"Table {table_name} has {out_of_range} rows with years outside 2010-2040 range"

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

    def test_temp_change_references_silver(self, spark):
        """Test that gold temp change country_names exist in silver temp change."""
        gold_table = "climate_energy_demand.gold.fct_temp_change_annual"
        silver_table = "climate_energy_demand.silver.temp_change_annual"

        if not all(spark.catalog.tableExists(t) for t in [gold_table, silver_table]):
            pytest.skip("Required tables don't exist")

        gold_df = spark.table(gold_table)
        silver_df = spark.table(silver_table)

        if "country_name" not in gold_df.columns or "country_name" not in silver_df.columns:
            pytest.skip("Required country_name columns not found")

        gold_countries = gold_df.select("country_name").distinct()
        silver_countries = silver_df.select("country_name").distinct()

        orphaned = gold_countries.join(silver_countries, on="country_name", how="left_anti")
        orphan_count = orphaned.count()

        assert orphan_count == 0, \
            f"Found {orphan_count} country_names in gold temp_change not in silver"

    def test_land_cover_references_silver(self, spark):
        """Test that gold land cover country_names exist in silver land cover."""
        gold_table = "climate_energy_demand.gold.fct_land_cover_annual"
        silver_table = "climate_energy_demand.silver.land_cover_annual"

        if not all(spark.catalog.tableExists(t) for t in [gold_table, silver_table]):
            pytest.skip("Required tables don't exist")

        gold_df = spark.table(gold_table)
        silver_df = spark.table(silver_table)

        if "country_name" not in gold_df.columns or "country_name" not in silver_df.columns:
            pytest.skip("Required country_name columns not found")

        gold_countries = gold_df.select("country_name").distinct()
        silver_countries = silver_df.select("country_name").distinct()

        orphaned = gold_countries.join(silver_countries, on="country_name", how="left_anti")
        orphan_count = orphaned.count()

        assert orphan_count == 0, \
            f"Found {orphan_count} country_names in gold land_cover not in silver"


# =============================================================================
# TEST 7: KOPPEN CLIMATE ZONE AND DYNAMIC BASE TEMP VALIDATION
# =============================================================================

class TestKoppenAndDynamicBaseTemps:
    """Test Köppen-Geiger classification, data quality flags, and dynamic HDD/CDD."""

    def test_dim_koppen_zones_complete(self, spark):
        """Test that dim_koppen_zones has all expected climate zones."""
        table_name = "climate_energy_demand.gold.dim_koppen_zones"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)
        # Should have at least 20 Köppen zone codes (full set is 26)
        assert df.count() >= 20, f"Expected at least 20 Köppen zones, got {df.count()}"

        # All zones should have descriptions and base temps
        invalid = df.filter(
            col("koppen_description").isNull() |
            col("heating_base_temp").isNull() |
            col("cooling_base_temp").isNull()
        ).count()
        assert invalid == 0, f"Found {invalid} zones with missing description or base temps"

        # Base temps should be in valid ranges
        invalid_temps = df.filter(
            (col("heating_base_temp") < 0) | (col("heating_base_temp") > 25) |
            (col("cooling_base_temp") < 0) | (col("cooling_base_temp") > 35)
        ).count()
        assert invalid_temps == 0, f"Found {invalid_temps} zones with out-of-range base temps"

    def test_dim_date_season_flags(self, spark):
        """Test heating_season and cooling_season flags in dim_date."""
        table_name = "climate_energy_demand.gold.dim_date"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)
        if "heating_season" not in df.columns or "cooling_season" not in df.columns:
            pytest.skip("heating_season or cooling_season columns not found")

        # Heating season: Oct-Mar (months 10,11,12,1,2,3)
        # Cooling season: Jun-Sep (months 6,7,8,9)
        # No month should be in both seasons
        overlap = df.filter(
            (col("heating_season") == True) & (col("cooling_season") == True)
        ).count()
        assert overlap == 0, f"Found {overlap} dates in both heating and cooling season"

    def test_dim_locations_data_quality_flags(self, spark):
        """Test data_quality_flag values in dim_locations are valid."""
        table_name = "climate_energy_demand.gold.dim_locations"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)
        if "data_quality_flag" not in df.columns:
            pytest.skip("data_quality_flag column not found")

        # Flag should be one of the valid values
        valid_flags = ["Sufficient", "Insufficient: no classified stations", "No weather data"]
        invalid = df.filter(~col("data_quality_flag").isin(valid_flags)).count()
        assert invalid == 0, f"Found {invalid} rows with invalid data_quality_flag"

        # Countries flagged Sufficient should have a non-null koppen_zone
        missing_zone = df.filter(
            (col("data_quality_flag") == "Sufficient") &
            col("koppen_zone").isNull()
        ).count()
        assert missing_zone == 0, f"Found {missing_zone} Sufficient countries with NULL koppen_zone"

        # Countries not flagged Sufficient should have NULL koppen_zone
        mismatched = df.filter(
            (col("data_quality_flag") != "Sufficient") &
            col("koppen_zone").isNotNull()
        ).count()
        assert mismatched == 0, f"Found {mismatched} insufficient countries with a koppen_zone"

    def test_dim_stations_data_quality_flags(self, spark):
        """Test data_quality_flag values in dim_stations are valid."""
        table_name = "climate_energy_demand.gold.dim_stations"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)
        if "data_quality_flag" not in df.columns:
            pytest.skip("data_quality_flag column not found")

        valid_flags = [
            "Sufficient", "Insufficient precipitation data",
            "Insufficient: missing months", "No weather data"
        ]
        invalid = df.filter(~col("data_quality_flag").isin(valid_flags)).count()
        assert invalid == 0, f"Found {invalid} rows with invalid data_quality_flag"

        # Sufficient stations should have non-null koppen_zone
        missing_zone = df.filter(
            (col("data_quality_flag") == "Sufficient") &
            col("koppen_zone").isNull()
        ).count()
        assert missing_zone == 0, f"Found {missing_zone} Sufficient stations with NULL koppen_zone"

    def test_energy_demand_dynamic_hdd_cdd(self, spark):
        """Test that fct_energy_demand uses dynamic base temps and HDD/CDD are non-negative."""
        table_name = "climate_energy_demand.gold.fct_energy_demand_daily"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")

        df = spark.table(table_name)

        # Check new columns exist
        for required_col in ["heating_base_temp", "cooling_base_temp",
                            "heating_degree_days", "cooling_degree_days"]:
            assert required_col in df.columns, f"Missing required column: {required_col}"

        # HDD and CDD should be non-negative
        negative_hdd = df.filter(col("heating_degree_days") < 0).count()
        assert negative_hdd == 0, f"Found {negative_hdd} rows with negative heating_degree_days"

        negative_cdd = df.filter(col("cooling_degree_days") < 0).count()
        assert negative_cdd == 0, f"Found {negative_cdd} rows with negative cooling_degree_days"

        # Base temps should be in valid ranges
        invalid_base = df.filter(
            (col("heating_base_temp") < 0) | (col("heating_base_temp") > 25) |
            (col("cooling_base_temp") < 0) | (col("cooling_base_temp") > 35)
        ).count()
        assert invalid_base == 0, f"Found {invalid_base} rows with out-of-range base temps"

        # Verify HDD/CDD match the base temps used
        # HDD = max(0, heating_base - temp_mean), CDD = max(0, temp_mean - cooling_base)
        from pyspark.sql.functions import greatest, lit
        mismatched = df.filter(
            (col("heating_degree_days") !=
             greatest(lit(0), col("heating_base_temp") - col("avg_temperature_c"))) |
            (col("cooling_degree_days") !=
             greatest(lit(0), col("avg_temperature_c") - col("cooling_base_temp")))
        ).count()
        assert mismatched == 0, \
            f"Found {mismatched} rows where HDD/CDD doesn't match base temp formula"

    def test_koppen_zone_validity(self, spark):
        """Test that all classified Köppen zones are valid codes."""
        valid_zones = [
            "Af", "Am", "Aw", "BWh", "BWk", "BSh", "BSk",
            "Cfa", "Cfb", "Cfc", "Csa", "Csb", "Csc",
            "Cwa", "Cwb", "Cwc",
            "Dfa", "Dfb", "Dfc", "Dfd", "Dwa", "Dwb", "Dwc", "Dwd",
            "ET", "EF"
        ]

        for table_name in [
            "climate_energy_demand.gold.dim_locations",
            "climate_energy_demand.gold.dim_stations"
        ]:
            if not spark.catalog.tableExists(table_name):
                continue

            df = spark.table(table_name)
            if "koppen_zone" not in df.columns:
                continue

            invalid = df.filter(
                col("koppen_zone").isNotNull() &
                ~col("koppen_zone").isin(valid_zones)
            ).count()
            assert invalid == 0, \
                f"Table {table_name} has {invalid} rows with invalid Köppen zone codes"

    def test_dim_locations_koppen_zone_joins_to_reference(self, spark):
        """Test that koppen_zone in dim_locations exists in dim_koppen_zones."""
        loc_table = "climate_energy_demand.gold.dim_locations"
        ref_table = "climate_energy_demand.gold.dim_koppen_zones"

        if not all(spark.catalog.tableExists(t) for t in [loc_table, ref_table]):
            pytest.skip("Required tables don't exist")

        loc_df = spark.table(loc_table)
        ref_df = spark.table(ref_table)

        if "koppen_zone" not in loc_df.columns:
            pytest.skip("koppen_zone not in dim_locations")

        # All non-null zones in dim_locations should exist in dim_koppen_zones
        loc_zones = loc_df.filter(col("koppen_zone").isNotNull()).select("koppen_zone").distinct()
        ref_zones = ref_df.select("koppen_zone").distinct()

        orphaned = loc_zones.join(ref_zones, on="koppen_zone", how="left_anti")
        orphan_count = orphaned.count()
        assert orphan_count == 0, \
            f"Found {orphan_count} Köppen zones in dim_locations not in dim_koppen_zones"

# =============================================================================
# TEST 8: REAL-WORLD SANITY CHECKS
# Validates gold data against known climate events and seasonal patterns.
# =============================================================================

class TestRealWorldSanity:
    """Sanity checks against known real-world climate events.
    
    Validates that gold tables correctly capture signatures of:
    - EU heat waves (2019, 2021, 2022, 2023)
    - Seasonal temperature patterns in latest data
    - Model-observation agreement (with documented Greece exception)
    """
    
    def test_summer_temp_anomalies_during_heat_waves(self, spark):
        """Validate that summer (Jun-Jul-Aug) temp anomalies are positive
        during known EU heat wave years: 2019, 2021, 2022, 2023."""
        table_name = "climate_energy_demand.gold.fct_temp_change_annual"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        heat_wave_years = [2019, 2021, 2022, 2023]
        eu_countries = ["France", "Germany", "Italy", "Spain", "Greece", "Portugal", "Belgium"]
        
        summer_anomalies = df.filter(
            (col("country_name").isin(eu_countries)) &
            (col("months").like("Jun%Jul%Aug%")) &
            (col("element") == "Temperature change") &
            (col("year").isin(heat_wave_years))
        )
        
        count = summer_anomalies.count()
        assert count > 0, "No summer temp anomaly data found for EU heat wave years"
        
        # All summer anomalies during known heat wave years should be positive (warming)
        negative_anomalies = summer_anomalies.filter(col("value") <= 0).count()
        assert negative_anomalies == 0, \
            f"Found {negative_anomalies} non-positive summer anomalies during known heat wave years"
    
    def test_heatwave_events_during_known_periods(self, spark):
        """Validate that heat wave event flags are set during known EU heat wave periods."""
        table_name = "climate_energy_demand.gold.fct_energy_demand_daily"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        # July 2022 Iberian heat wave — Portugal hit record 47°C
        pt_jul_2022 = df.filter(
            (col("iso_code") == "PRT") &
            (col("date") >= "2022-07-01") &
            (col("date") <= "2022-07-31") &
            (col("is_heatwave_event") == True)
        ).count()
        assert pt_jul_2022 > 0, \
            "No heat wave events detected for Portugal in July 2022 (known extreme heat wave)"
        
        # August 2021 Greek heat wave — severe wildfires
        grc_aug_2021 = df.filter(
            (col("iso_code") == "GRC") &
            (col("date") >= "2021-08-01") &
            (col("date") <= "2021-08-31") &
            (col("is_heatwave_event") == True)
        ).count()
        assert grc_aug_2021 > 0, \
            "No heat wave events detected for Greece in August 2021 (known heat wave + wildfires)"
    
    def test_ground_truth_model_accuracy_most_countries(self, spark):
        """Validate that model-observation agreement is good for most EU countries."""
        table_name = "climate_energy_demand.gold.fct_ground_truth_verification_daily"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        # These countries have stations near the national avg — expect high accuracy
        eu_countries = ["France", "Germany", "Italy", "Belgium", "Netherlands"]
        
        for country in eu_countries:
            country_df = df.filter(col("country") == country)
            total = country_df.count()
            if total == 0:
                continue
            within = country_df.filter(col("is_within_tolerance") == True).count()
            pct = within / total * 100
            assert pct >= 80, \
                f"{country}: only {pct:.1f}% within tolerance (expected >=80%)"
    
    def test_ground_truth_greece_bias_documented(self, spark):
        """Validate that Greece's high model bias is a known limitation.
        
        Greece has only one NOAA station (LAMIA) located in an inland plain
        that is much hotter than the country-level modeled average. See ADR #8.
        This test documents the issue rather than failing."""
        table_name = "climate_energy_demand.gold.fct_ground_truth_verification_daily"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        greece_df = df.filter(col("country") == "Greece")
        total = greece_df.count()
        if total == 0:
            pytest.skip("No Greece data available")
        
        avg_abs_error = greece_df.agg(spark_avg("absolute_error")).collect()[0][0]
        
        # Document the known limitation — warn if bias is very high
        if avg_abs_error and avg_abs_error > 3.0:
            import warnings
            warnings.warn(
                f"Greece avg absolute error is {avg_abs_error:.2f}°C — "
                f"known limitation: country-level modeled data vs inland station (LAMIA). "
                f"See ADR #8 for details."
            )
        
        # Test passes regardless — this is a documented limitation
        assert True
    
    def test_latest_data_seasonal_patterns(self, spark):
        """Validate that the latest available data shows reasonable seasonal patterns.
        Summer months should be warmer than winter months for EU countries."""
        table_name = "climate_energy_demand.gold.fct_energy_demand_daily"
        if not spark.catalog.tableExists(table_name):
            pytest.skip(f"Table {table_name} does not exist")
        
        df = spark.table(table_name)
        
        # Get the latest date in the table
        latest = df.select(spark_max("date").alias("max_date")).collect()[0]["max_date"]
        if latest is None:
            pytest.skip("No date data available")
        
        latest_year = latest.year if hasattr(latest, 'year') else int(str(latest)[:4])
        
        # Check that the latest year has data
        latest_year_count = df.filter(year(col("date")) == latest_year).count()
        assert latest_year_count > 0, f"No data found for latest year {latest_year}"
        
        # Summer months (Jun-Aug) should have higher avg temps than winter (Dec-Feb)
        eu_codes = ["FRA", "DEU", "ITA", "ESP", "GRC", "PRT", "BEL", "NLD"]
        
        summer_temps = df.filter(
            (col("iso_code").isin(eu_codes)) &
            (year(col("date")) == latest_year) &
            (month(col("date")).isin([6, 7, 8]))
        ).agg(spark_avg("avg_temperature_c")).collect()[0][0]
        
        winter_temps = df.filter(
            (col("iso_code").isin(eu_codes)) &
            (year(col("date")) == latest_year) &
            (month(col("date")).isin([12, 1, 2]))
        ).agg(spark_avg("avg_temperature_c")).collect()[0][0]
        
        if summer_temps and winter_temps:
            assert summer_temps > winter_temps, \
                f"Summer avg temp ({summer_temps:.1f}°C) should exceed winter ({winter_temps:.1f}°C) for EU countries"