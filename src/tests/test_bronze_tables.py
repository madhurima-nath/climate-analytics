# test_bronze_tables.py
# Purpose: Bronze layer validation - data existence, freshness, and volume completeness
# Run via: pytest test_bronze_tables.py -v --tb=short --assert=plain --import-mode=importlib

import pytest
from datetime import datetime, timedelta

# ============================================================================
# Configuration
# ============================================================================

CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads"
STALENESS_THRESHOLD_DAYS = 7

# All 25 expected bronze tables
EXPECTED_TABLES = [
    # FAO Land Cover (6 tables)
    "fao_land_cover_all_data",
    "fao_land_cover_area_codes",
    "fao_land_cover_data_noflag",
    "fao_land_cover_elements",
    "fao_land_cover_flags",
    "fao_land_cover_item_codes",
    # FAO Land Use (6 tables)
    "fao_land_use_all_data",
    "fao_land_use_area_codes",
    "fao_land_use_data_noflag",
    "fao_land_use_elements",
    "fao_land_use_flags",
    "fao_land_use_item_codes",
    # FAO Temperature Change (5 tables)
    "fao_temp_change_all_data",
    "fao_temp_change_area_codes",
    "fao_temp_change_data_noflag",
    "fao_temp_change_elements",
    "fao_temp_change_flags",
    # GFW Forest (4 tables)
    "gfw_emissions_polygons",
    "gfw_net_flux",
    "gfw_peatlands",
    "gfw_tropical_tree_cover",
    # API-ingested (3 tables - no volume files)
    "noaa_gsod",
    "openmeteo_climate_cmip6_projections",
    "openmeteo_weather",
    # OWID Energy (1 table)
    "owid_energy",
]

# Main data tables (subject to freshness checks - exclude small lookup/code tables)
MAIN_TABLES = [
    "fao_land_cover_all_data",
    "fao_land_use_all_data",
    "fao_temp_change_all_data",
    "gfw_emissions_polygons",
    "gfw_net_flux",
    "gfw_peatlands",
    "gfw_tropical_tree_cover",
    "noaa_gsod",
    "openmeteo_climate_cmip6_projections",
    "openmeteo_weather",
    "owid_energy",
]

# Mapping: volume CSV files -> bronze table names
# Files not in this map are flagged as unmapped (potential new uploads)
# Tables without files here (noaa_gsod, openmeteo_*) are API-ingested, not volume-loaded
FILE_TO_TABLE = {
    "Environment_LandCover_All_Data.csv": "fao_land_cover_all_data",
    "Environment_LandCover_All_Data_NOFLAG.csv": "fao_land_cover_data_noflag",
    "Environment_LandCover_AreaCodes.csv": "fao_land_cover_area_codes",
    "Environment_LandCover_Elements.csv": "fao_land_cover_elements",
    "Environment_LandCover_Flags.csv": "fao_land_cover_flags",
    "Environment_LandCover_ItemCodes.csv": "fao_land_cover_item_codes",
    "Environment_Temperature_change_All_Data.csv": "fao_temp_change_all_data",
    "Environment_Temperature_change_All_Data_NOFLAG.csv": "fao_temp_change_data_noflag",
    "Environment_Temperature_change_AreaCodes.csv": "fao_temp_change_area_codes",
    "Environment_Temperature_change_Elements.csv": "fao_temp_change_elements",
    "Environment_Temperature_change_Flags.csv": "fao_temp_change_flags",
    "Forest_greenhouse_gas_emissions_polygons.csv": "gfw_emissions_polygons",
    "Forest_greenhouse_gas_net_flux.csv": "gfw_net_flux",
    "Global_Peatlands.csv": "gfw_peatlands",
    "Inputs_LandUse_All_Data.csv": "fao_land_use_all_data",
    "Inputs_LandUse_All_Data_NOFLAG.csv": "fao_land_use_data_noflag",
    "Inputs_LandUse_AreaCodes.csv": "fao_land_use_area_codes",
    "Inputs_LandUse_Elements.csv": "fao_land_use_elements",
    "Inputs_LandUse_Flags.csv": "fao_land_use_flags",
    "Inputs_LandUse_ItemCodes.csv": "fao_land_use_item_codes",
    "Tropical_Tree_Cover.csv": "gfw_tropical_tree_cover",
    "owid-energy-data.csv": "owid_energy",
    "reference_locations.csv": None,  # Reference file, no bronze table needed
}


def _full_name(table):
    return f"{CATALOG}.{SCHEMA}.{table}"


# ============================================================================
# Test 1: Bronze tables exist and have data
# ============================================================================

def test_bronze_tables_exist(spark):
    """All expected bronze tables exist in Unity Catalog."""
    missing = []
    for table in EXPECTED_TABLES:
        if not spark.catalog.tableExists(_full_name(table)):
            missing.append(table)
    assert not missing, f"Missing bronze tables: {', '.join(missing)}"


def test_bronze_tables_have_rows(spark):
    """All main bronze tables have at least 1 row (not empty)."""
    empty = []
    for table in MAIN_TABLES:
        try:
            count = spark.table(_full_name(table)).count()
            if count == 0:
                empty.append(table)
        except Exception as e:
            empty.append(f"{table} (error: {e})")
    assert not empty, f"Empty bronze tables: {', '.join(empty)}"


# ============================================================================
# Test 2: Data freshness (staleness within 7 days)
# ============================================================================

def test_bronze_data_freshness(spark):
    """All main bronze tables were last written within the last 7 days."""
    threshold = datetime.now() - timedelta(days=STALENESS_THRESHOLD_DAYS)
    stale = []
    for table in MAIN_TABLES:
        try:
            history = spark.sql(f"DESCRIBE HISTORY {_full_name(table)} LIMIT 1")
            last_write = history.collect()[0]["timestamp"]
            # Convert to datetime if needed
            if hasattr(last_write, "to_pydatetime"):
                last_write = last_write.to_pydatetime()
            elif hasattr(last_write, "year"):
                pass  # Already datetime
            if last_write < threshold:
                stale.append(f"{table} (last: {last_write.strftime('%Y-%m-%d')})")
        except Exception as e:
            stale.append(f"{table} (history error: {e})")
    assert not stale, (
        f"Stale bronze tables (>{STALENESS_THRESHOLD_DAYS} days since last write): "
        f"{', '.join(stale)}"
    )


# ============================================================================
# Test 3: Volume file completeness (all raw files ingested to UC)
# ============================================================================

def test_volume_files_ingested(spark):
    """All CSV files in raw_uploads volume have corresponding bronze UC tables."""
    # List all files in the volume
    files_df = spark.sql(f"LIST '{VOLUME_PATH}'")
    csv_files = [row.name for row in files_df.collect() if row.name.endswith(".csv")]

    # Get all existing bronze table names
    existing_tables = set()
    tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
    for row in tables_df.collect():
        existing_tables.add(row.tableName)

    # Check each file against the mapping
    not_ingested = []
    unmapped_files = []
    for filename in csv_files:
        if filename not in FILE_TO_TABLE:
            unmapped_files.append(filename)
            continue
        expected_table = FILE_TO_TABLE[filename]
        if expected_table is None:
            continue  # Reference file, no table needed
        if expected_table not in existing_tables:
            not_ingested.append(f"{filename} -> {expected_table}")

    issues = []
    if not_ingested:
        issues.append(f"Files without UC tables: {', '.join(not_ingested)}")
    if unmapped_files:
        issues.append(
            f"Unmapped files (new uploads not yet configured): "
            f"{', '.join(unmapped_files)}"
        )
    assert not issues, " | ".join(issues)