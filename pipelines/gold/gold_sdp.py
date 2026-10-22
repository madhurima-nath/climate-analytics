# =============================================================================
# Gold Layer: Lakeflow Spark Declarative Pipeline (SDP) Source
# =============================================================================
# This file defines the Gold-layer materialized views using the SDP Python API.
# It coexists with the notebook-based gold_orchestrator and writes to a
# separate schema (gold_sdp) to avoid table conflicts.
#
# Pattern: @dlt.table (batch reads via dlt.read create materialized views)
#   - Reads from Silver SDP tables via dlt.read()
#   - Transform functions from src/transforms/*.py are reused as-is
#   - Automatic dependency resolution replaces job task chaining
#   - Materialized view refresh replaces the notebook's full-overwrite pattern
#
# Dependency order is resolved automatically by SDP:
#   dim_date, dim_stations, dim_locations, dim_koppen_zones (dimensions first)
#   → fct_energy_demand, fct_temp_change, fct_land_cover, fct_forest_resilience, fct_ground_truth (facts)
#
# See: pipeline_approaches.md for the comparison with notebook orchestration.
# =============================================================================

import os
import sys

# Path setup: make src/ importable inside the SDP pipeline. __file__ is not
# defined in SDP's module loader, so fall back to the bundle root (Git folder).
try:
    _project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
except NameError:
    _project_root = "/Workspace/Users/madhurima.nath@icloud.com/climate-analytics"
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import dlt

from src.transforms.climate import (
    transform_gold_dim_stations,
    transform_gold_dim_locations,
    transform_gold_dim_date,
    transform_dim_koppen_zones,
)
from src.transforms.energy import process_energy_demand
from src.transforms.nature import (
    process_temp_change_gold,
    process_land_cover_gold,
    process_forest_resilience,
)
from src.transforms.quality import process_fidelity_audit

import src.transforms.climate as _climate
from pyspark.sql.functions import udf


# UDF rebinding (SDP-only): the Koppen classification UDF in climate.py wraps a
# module-level function, which cloudpickle serializes by reference to
# `src.transforms.climate`. The pipeline's Python workers cannot import `src`,
# so the UDF fails with ModuleNotFoundError. Rebinding it here — with the import
# done inside the wrapper at call time — makes cloudpickle serialize it by
# value, the standard pattern for UDFs defined in pipeline sources. Only this
# pipeline process is affected; the notebook orchestrators keep the original.
def _koppen_with_dq_sdp(months, temps, precips, coverage_flags):
    import sys as _sys
    _root = "/Workspace/Users/madhurima.nath@icloud.com/climate-analytics"
    if _root not in _sys.path:
        _sys.path.insert(0, _root)
    from src.transforms.climate import _koppen_with_dq
    return _koppen_with_dq(months, temps, precips, coverage_flags)


_climate._koppen_with_dq_udf = udf(_koppen_with_dq_sdp, _climate._koppen_result_schema)

# Schema prefix for reading Silver SDP tables
_SILVER = "climate_energy_demand.silver_sdp"

# -----------------------------------------------------------------------------
# DIMENSIONS (processed first — SDP resolves dependencies automatically)
# -----------------------------------------------------------------------------

# 1. dim_date — promoted from silver.dim_date
@dlt.table(
    name="climate_energy_demand.gold_sdp.dim_date",
    comment="Master temporal dimension promoted from silver for star-schema join integrity (SDP).",
)
def dim_date():
    sources = {"dim_date_source": dlt.read(f"{_SILVER}.dim_date")}
    return transform_gold_dim_date(sources, {})

# 2. dim_stations — promoted from silver.dim_stations + weather_observations
@dlt.table(
    name="climate_energy_demand.gold_sdp.dim_stations",
    comment="Master weather station lookup with Koppen zone enrichment (SDP).",
)
def dim_stations():
    sources = {
        "dim_stations_source": dlt.read(f"{_SILVER}.dim_stations"),
        "weather_source": dlt.read(f"{_SILVER}.weather_observations"),
    }
    return transform_gold_dim_stations(sources, {})

# 3. dim_locations — promoted from silver.dim_locations + weather_observations
@dlt.table(
    name="climate_energy_demand.gold_sdp.dim_locations",
    comment="Master country lookup with Koppen zone enrichment and dynamic base temperatures (SDP).",
)
def dim_locations():
    sources = {
        "dim_locations_source": dlt.read(f"{_SILVER}.dim_locations"),
        "weather_source": dlt.read(f"{_SILVER}.weather_observations"),
    }
    return transform_gold_dim_locations(sources, {})

# 4. dim_koppen_zones — generated from Python dicts (no source table)
@dlt.table(
    name="climate_energy_demand.gold_sdp.dim_koppen_zones",
    comment="Koppen-Geiger climate zone reference with zone-specific HDD/CDD base temperatures (SDP).",
)
def dim_koppen_zones():
    sources = {}
    return transform_dim_koppen_zones(sources, {})

# -----------------------------------------------------------------------------
# FACT TABLES (depend on dimensions above — SDP resolves order)
# -----------------------------------------------------------------------------

# 5. fct_energy_demand_daily — reads silver weather + energy + dim_date, and gold dim_locations
@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_energy_demand_daily",
    comment="Daily energy demand joined with weather stressors and national energy baselines (SDP).",
)
def fct_energy_demand_daily():
    sources = {
        "weather_source": dlt.read(f"{_SILVER}.weather_historical"),
        "energy_source": dlt.read(f"{_SILVER}.energy_metrics"),
        "date_dim_source": dlt.read(f"{_SILVER}.dim_date"),
        "location_dim_source": dlt.read("climate_energy_demand.gold_sdp.dim_locations"),
    }
    return process_energy_demand(sources, {"z_order_columns": ["iso_code", "date"]})

# 6. fct_temp_change_annual — reads silver temp_change + dim_locations
@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_temp_change_annual",
    comment="Annual temperature change anomalies enriched with ISO codes (SDP).",
)
def fct_temp_change_annual():
    sources = {
        "temp_change_source": dlt.read(f"{_SILVER}.temp_change_annual"),
        "location_dim_source": dlt.read(f"{_SILVER}.dim_locations"),
    }
    return process_temp_change_gold(sources, {"partition_by": ["year"]})

# 7. fct_land_cover_annual — reads silver land_cover + dim_locations
@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_land_cover_annual",
    comment="Annual national land cover by category and satellite source (SDP).",
)
def fct_land_cover_annual():
    sources = {
        "land_cover_source": dlt.read(f"{_SILVER}.land_cover_annual"),
        "location_dim_source": dlt.read(f"{_SILVER}.dim_locations"),
    }
    return process_land_cover_gold(sources, {"partition_by": ["year"]})

# 8. fct_forest_resilience_annual — reads silver forest + weather + dim_locations + land_cover
@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_forest_resilience_annual",
    comment="Annual forest carbon health vs climate stressors at country grain (SDP).",
)
def fct_forest_resilience_annual():
    sources = {
        "forest_source": dlt.read(f"{_SILVER}.forest_inventory_annual"),
        "weather_source": dlt.read(f"{_SILVER}.weather_historical"),
        "location_dim_source": dlt.read(f"{_SILVER}.dim_locations"),
        "land_cover_source": dlt.read(f"{_SILVER}.land_cover_annual"),
    }
    return process_forest_resilience(sources, {"partition_by": ["year"], "z_order_columns": ["iso_code"]})

# 9. fct_ground_truth_verification_daily — reads silver weather_observations + weather_historical + dim_stations
@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_ground_truth_verification_daily",
    comment="DQ audit comparing physical NOAA station observations vs Open-Meteo modelled data (SDP).",
)
def fct_ground_truth_verification_daily():
    sources = {
        "physical_observations": dlt.read(f"{_SILVER}.weather_observations"),
        "modeled_data": dlt.read(f"{_SILVER}.weather_historical"),
        "station_dim": dlt.read(f"{_SILVER}.dim_stations"),
    }
    return process_fidelity_audit(sources, {"z_order_columns": ["station_id", "date"]})