# Silver Layer Unit Tests & Validation

Comprehensive test suite for validating the Climate Energy Demand silver layer tables.

## Test Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                 DAB Job: silver_validation                      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  run_all_tests_notebook.py   │
              │  (Serverless Compute)        │
              └──────────┬───────────────────┘
                         │
                         ▼
              ┌──────────────────────────────┐
              │  🔍 Check if validation     │
              │     needed                   │
              │                              │
              │  Compare timestamps:         │
              │  • Last data load            │
              │  • Last validation           │
              └──────────┬───────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
          ▼                             ▼
    ⏭️ SKIP                      ✅ RUN TESTS
    No data changes              Data has changed
                                       │
                         ┌─────────────┼─────────────┐
                         │             │             │
                         ▼             ▼             ▼
                 ┌───────────┐ ┌───────────┐ ┌───────────┐
                 │test_silver│ │test_audit │ │test_shared│
                 │_tables.py │ │_utils.py  │ │_logic.py  │
                 │           │ │           │ │           │
                 │• Tables   │ │• Watermark│ │• Thermal  │
                 │• Data     │ │• Audit log│ │• H3 index │
                 │• Quality  │ │           │ │• Unpivot  │
                 └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
                       │             │             │
                       └─────────────┼─────────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  Test Summary Report │
                          │  • Pass/Fail counts  │
                          │  • HTML report       │
                          │  • Exit code (0/1)   │
                          └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  📝 Update Validation│
                          │     Timestamp        │
                          │  (in audit table)    │
                          └──────────────────────┘
```

**Smart Validation:** Tests automatically skip if no data has changed since the last validation run. This is determined by comparing:
- `MAX(processed_at)` from audit table (last data load)
- `last_watermark` for `__validation_metadata__` row (last validation run)

If `last_data_load <= last_validation`, tests are skipped to save compute resources.

## Overview

This test suite provides **intelligent, incremental validation** that only runs when data changes.

### Key Features
* ⚡ **Conditional Execution**: Skips validation if no data changes detected
* 🔍 **Watermark-Based**: Compares data load timestamps vs last validation
* 💰 **Cost-Efficient**: Saves compute by avoiding redundant validation
* 🔧 **Force-Run Option**: Can force validation by deleting metadata row

### What It Validates
- ✅ **Table Existence**: All 10 silver tables exist
- ✅ **Table Population**: Tables contain data with reasonable row counts
- ✅ **Module Imports**: All transform and utility modules load correctly
- ✅ **Null Checks**: Key columns (primary keys) have no unexpected nulls
- ✅ **Unit Conversions**: Temperature is in Celsius (reasonable -60°C to 60°C range)
- ✅ **Thermal Stress Calculations**: HDD/CDD logic (base 15°C heating, 25°C cooling)
- ✅ **Deduplication**: No duplicate primary key combinations
- ✅ **MERGE Logic**: Audit watermarking is functioning
- ✅ **Geospatial Indexing**: H3 hexagon indexing at resolution 6
- ✅ **Data Quality**: Year ranges (≥2010), date continuity, dimension completeness
- ✅ **Relational Normalisation**: Wide-to-long format transformation (y1990 → observation_year)

## Test Files

### 1. `test_silver_tables.py` (Main Test Suite)

Comprehensive validation of all 10 silver tables:

**Tables Tested:**
1. `energy_metrics` - National energy demand, generation, GDP, population
2. `weather_observations` - NOAA GSOD ground truth observations
3. `weather_projections` - Climate model forecasts
4. `weather_historical` - Historical weather reconstructions
5. `dim_stations` - Station metadata dimension
6. `dim_h3_grid` - H3 hexagonal grid spatial dimension
7. `dim_date` - Date dimension calendar
8. `dim_locations` - Country/location dimension
9. `carbon_flux_spatial` - Carbon flux spatial measurements
10. `forest_inventory_annual` - Annual forest inventory data

**Test Classes:**
- `TestTableExistence` - Verify all tables exist and have data
- `TestImports` - Validate Python module imports
- `TestNullValues` - Check for unexpected nulls in key columns
- `TestUnitConversions` - Verify temperature conversions and thermal stress calculations
- `TestMergeAndDedup` - Validate MERGE logic and primary key uniqueness
- `TestGeospatialFunctions` - Test H3 geospatial indexing
- `TestDataQuality` - Spot checks for year ranges, dates, dimension completeness
- `TestRelationalNormalisation` - Test wide-to-long unpivot transformation

### 2. `test_audit_utils.py`

Tests for watermark and audit logging functionality:
- Audit table existence
- Default watermark handling (epoch 1900-01-01 for new tables)
- Watermark retrieval for existing tables
- Audit log record creation
- Required column validation

### 3. `test_shared_logic.py`

Tests for common transformation logic:
- `calculate_thermal_stress()` - Heating/Cooling degree days
- `geospatial_indexing()` - H3 hexagon assignment
- `relational_normalisation()` - Wide-to-long pivot

## Running Tests

### Option 1: Using the Python Test Runner (Recommended)

```bash
cd /Workspace/Users/<your-email>/climate-analytics
python src/tests/run_all_tests.py
```

This script:
1. Auto-installs pytest if needed
2. Configures the test environment
3. Runs all test suites with verbose output
4. Generates HTML test reports
5. Provides a comprehensive summary
6. Returns proper exit codes for CI/CD integration

### Option 2: Using pytest directly (from workspace terminal)

```bash
cd /Workspace/Users/<your-email>/climate-analytics

# Run all tests
pytest src/tests/ -v

# Run specific test file
pytest src/tests/test_silver_tables.py -v

# Run specific test class
pytest src/tests/test_silver_tables.py::TestTableExistence -v

# Run with HTML report
pytest src/tests/ -v --html=test_report.html --self-contained-html
```

### Option 3: Interactive Notebook (Alternative)

For interactive cell-by-cell execution, you can use the `run_tests` notebook:
```
/Workspace/Users/<your-email>/climate-analytics/src/tests/run_tests
```

Note: This is a Databricks notebook (not `.ipynb` in the workspace), useful for interactive exploration.

### Option 4: Python Notebook Cell

```python
import pytest
import sys
import os

# Configure path
project_root = "/Workspace/Users/<your-email>/climate-analytics"
sys.path.insert(0, project_root)

# Run tests
pytest.main([
    f"{project_root}/src/tests/test_silver_tables.py",
    "-v",
    "--tb=short"
])
```

## Key Validations

### Unit Conversion Validation

**Temperature Check:**
- Validates temperatures are in Celsius (not Fahrenheit)
- Expected range: -60°C to 60°C
- Tests would fail if temperatures are in Fahrenheit (e.g., 95°F would be outside range)

**Thermal Stress Calculations:**
```python
# Heating Degree Days (HDD) = max(0, 15 - temperature)
Temp 10°C → HDD = 5.0 ✓
Temp 15°C → HDD = 0.0 ✓

# Cooling Degree Days (CDD) = max(0, temperature - 25)
Temp 25°C → CDD = 0.0 ✓
Temp 30°C → CDD = 5.0 ✓
```

### MERGE & Deduplication Validation

**Primary Key Uniqueness:**
- Compares total row count vs distinct key combinations
- Ensures no duplicate records after MERGE operations
- Validates merge keys from config files (e.g., `[iso_code, year]`, `[station_id, date]`)

**Audit Watermarking:**
- Verifies `ingestion_audit` table is populated
- Checks that silver tables have corresponding audit records
- Validates incremental processing is tracked

### Data Quality Spot Checks

**Year Range Validation:**
```python
# energy_metrics should have year >= 2010 (per config)
min_year >= 2010 ✓
min_year >= 1900 (sanity check) ✓
max_year <= 2100 (sanity check) ✓
```

**Date Dimension Completeness:**
- Calculates expected number of days in date range
- Verifies at least 95% of dates are present (allows small gaps)
- Ensures no large gaps in calendar dimension

### Geospatial Validation

**H3 Indexing:**
- Tests H3 resolution 6 hexagon assignment
- Validates known coordinates (NYC: 40.7N, -74.0W)
- Checks `dim_h3_grid` has no null H3 indices

## Interpreting Results

### Successful Test Run
```
============================= test session starts =============================
collected 45 items

test_silver_tables.py::TestTableExistence::test_table_exists[energy_metrics] PASSED
test_silver_tables.py::TestTableExistence::test_table_has_data[energy_metrics] PASSED
...

============================= 45 passed in 12.34s =============================
✅ All silver table tests PASSED!
```

### Failed Test Examples

**Missing Table:**
```
FAILED test_silver_tables.py::test_table_exists[energy_metrics]
AssertionError: Silver table climate_energy_demand.silver.energy_metrics does not exist
```

**Duplicate Primary Keys:**
```
FAILED test_silver_tables.py::test_no_duplicate_primary_keys[weather_observations]
AssertionError: Table has 10000 rows but only 9500 distinct key combinations. Duplicates detected!
```

**Temperature Not Converted:**
```
FAILED test_silver_tables.py::test_temperature_conversion_weather_observations
AssertionError: Maximum temperature 95.0 seems too hot (possibly still in Fahrenheit?)
```

## Customization

### Adjusting Expected Row Counts

Edit `MIN_ROW_COUNTS` dictionary in `test_silver_tables.py`:

```python
MIN_ROW_COUNTS = {
    "climate_energy_demand.silver.energy_metrics": 500,  # Increase minimum
    # ... other tables
}
```

### Adding New Key Columns

Edit `KEY_COLUMNS` dictionary for null validation:

```python
KEY_COLUMNS = {
    "climate_energy_demand.silver.energy_metrics": ["iso_code", "year", "new_key"],
    # ... other tables
}
```

### Adding New Tables

Add to `EXPECTED_SILVER_TABLES` list:

```python
EXPECTED_SILVER_TABLES = [
    # ... existing tables
    "climate_energy_demand.silver.new_table_name"
]
```

## Troubleshooting

### Import Errors

**Problem:** `ModuleNotFoundError: No module named 'src'`

**Solution:** Ensure project root is in Python path:
```python
import sys
project_root = "/Workspace/Users/<your-email>/climate-analytics"
sys.path.insert(0, project_root)
```

### Table Not Found

**Problem:** `Table or view not found: climate_energy_demand.silver.xxx`

**Solution:** 
1. Check table exists: `spark.catalog.tableExists("climate_energy_demand.silver.xxx")`
2. Verify silver orchestrator ran successfully
3. Check Unity Catalog permissions

### Test Skipped

**Problem:** `SKIPPED [1] test_silver_tables.py:123: No key columns defined`

**Solution:** This is expected - add key columns to `KEY_COLUMNS` dictionary if needed

## CI/CD Integration

To integrate these tests into your CI/CD pipeline, add a workflow task:

```yaml
# In databricks.yml
resources:
  jobs:
    test_silver_layer:
      name: "Test Silver Layer"
      tasks:
        - task_key: run_unit_tests
          python_file_task:
            python_file: "./src/tests/run_all_tests.py"
```

Alternatively, use the notebook for interactive testing:
```yaml
        - task_key: run_unit_tests
          notebook_task:
            notebook_path: "./src/tests/run_tests"
```

## Next Steps

1. ✅ **Run Tests:** Execute `python src/tests/run_all_tests.py` to validate current state
2. 📊 **Review Results:** Check HTML report for any failures
3. 🔧 **Fix Issues:** Address any failing tests
4. 🔄 **Automate:** Add test job to DAB pipeline
5. 📈 **Expand:** Add more tests as pipeline grows

## Contact

For questions about these tests, refer to:
- Silver orchestrator: `pipelines/silver/silver_orchestrator.py`
- Transform functions: `src/transforms/*.py`
- Config files: `pipelines/silver/configs/*.yml`