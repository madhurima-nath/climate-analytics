# =============================================================================
# Silver Layer: Lakeflow Spark Declarative Pipeline (SDP) Source
# =============================================================================
# This file defines the Silver-layer streaming tables using the SDP Python API.
# It coexists with the notebook-based silver_orchestrator and writes to a
# separate schema (silver_sdp) to avoid table conflicts.
#
# Pattern: @dlt.table (streaming read creates a streaming table) -> dlt.apply_changes (upsert)
#   - Streaming reads from Bronze Delta tables via spark.readStream.table()
#   - Transform functions from src/transforms/*.py are reused as-is
#   - apply_changes replaces the notebook orchestrator's MERGE logic
#   - Automatic SDP checkpointing replaces manual watermark tracking
#
# See: pipeline_approaches.md for the comparison with notebook orchestration.
# =============================================================================

import os
import sys

# Path setup: make src/ importable when running inside the SDP pipeline.
# SDP executes source files through its own loader, where __file__ is not defined,
# so fall back to the bundle root (this pipeline's sources live in the Git folder).
try:
    _project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
except NameError:
    _project_root = "/Workspace/Users/madhurima.nath@icloud.com/climate-analytics"
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

from src.transforms.weather import (
    process_weather_historical,
    process_weather_observations,
    process_weather_projections,
    create_dim_stations,
)
from src.transforms.energy import process_energy_metrics
from src.transforms.nature import (
    process_forest_inventory,
    process_land_cover,
    process_temp_change,
)
from src.transforms.common import create_dim_date, create_dim_locations

# -----------------------------------------------------------------------------
# 1. weather_historical
#    Source: bronze.openmeteo_weather | Keys: country, date
# -----------------------------------------------------------------------------

# Materialized view (batch read): process_weather_historical deduplicates by
# country/date with a window and forward-fills gaps across the full history —
# operations that need a full-scan view and cannot run on a stream. Dataset type
# follows source semantics (see pipeline_approaches.md).
@dlt.table(
    name="climate_energy_demand.silver_sdp.weather_historical",
    comment="Daily temperature observations with HDD/CDD (SDP).",
)
def weather_historical():
    sources = {"raw_weather": spark.read.table("climate_energy_demand.bronze.openmeteo_weather")}
    return process_weather_historical(sources, {"hdd_base": 15, "cdd_base": 25, "h3_resolution": 6})

# -----------------------------------------------------------------------------
# 2. weather_observations
#    Source: bronze.noaa_gsod | Keys: country, station_id, date
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.weather_observations_raw",
    comment="Intermediate streaming table for weather_observations (SDP).",
)
def weather_observations_raw():
    sources = {"noaa_raw": spark.readStream.table("climate_energy_demand.bronze.noaa_gsod")}
    df = process_weather_observations(sources, {"unit_standard": "metric"})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.weather_observations",
    comment="Metric-standardised NOAA GSOD ground-truth observations (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.weather_observations",
    source="climate_energy_demand.silver_sdp.weather_observations_raw",
    keys=["country", "station_id", "date"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 3. weather_projections
#    Source: bronze.openmeteo_climate_cmip6_projections | Keys: model, country, date, latitude, longitude
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.weather_projections_raw",
    comment="Intermediate streaming table for weather_projections (SDP).",
)
def weather_projections_raw():
    sources = {"raw_weather": spark.readStream.table("climate_energy_demand.bronze.openmeteo_climate_cmip6_projections")}
    df = process_weather_projections(sources, {"hdd_base": 15, "cdd_base": 25, "h3_resolution": 6})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.weather_projections",
    comment="CMIP6 climate projections with HDD/CDD for energy demand forecasting (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.weather_projections",
    source="climate_energy_demand.silver_sdp.weather_projections_raw",
    keys=["model", "country", "date", "latitude", "longitude"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 4. energy_metrics
#    Source: bronze.owid_energy | Keys: iso_code, year
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.energy_metrics_raw",
    comment="Intermediate streaming table for energy_metrics (SDP).",
)
def energy_metrics_raw():
    sources = {"owid_raw": spark.readStream.table("climate_energy_demand.bronze.owid_energy")}
    df = process_energy_metrics(sources, {"start_year": 2010})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.energy_metrics",
    comment="Annual electricity demand, generation, and socio-economic indicators (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.energy_metrics",
    source="climate_energy_demand.silver_sdp.energy_metrics_raw",
    keys=["iso_code", "year"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 5. forest_inventory_annual
#    Sources: bronze.fao_land_use_data_noflag, bronze.fao_land_use_item_codes
#    Keys: country_name, year, land_use_category, unit
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.forest_inventory_annual_raw",
    comment="Intermediate streaming table for forest_inventory_annual (SDP).",
)
def forest_inventory_annual_raw():
    sources = {
        "fao_data": spark.readStream.table("climate_energy_demand.bronze.fao_land_use_data_noflag"),
        "fao_items": spark.readStream.table("climate_energy_demand.bronze.fao_land_use_item_codes"),
    }
    df = process_forest_inventory(sources, {"start_year": 2010, "unit_standard": "ha"})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.forest_inventory_annual",
    comment="Annual national forest cover and land-use metrics from FAO (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.forest_inventory_annual",
    source="climate_energy_demand.silver_sdp.forest_inventory_annual_raw",
    keys=["country_name", "year", "land_use_category", "unit"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 6. land_cover_annual
#    Source: bronze.fao_land_cover_data_noflag | Keys: country_name, year, land_cover_category, element, unit
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.land_cover_annual_raw",
    comment="Intermediate streaming table for land_cover_annual (SDP).",
)
def land_cover_annual_raw():
    sources = {"fao_data": spark.readStream.table("climate_energy_demand.bronze.fao_land_cover_data_noflag")}
    df = process_land_cover(sources, {"start_year": 2010})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.land_cover_annual",
    comment="Annual national land cover metrics by satellite source from FAO (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.land_cover_annual",
    source="climate_energy_demand.silver_sdp.land_cover_annual_raw",
    keys=["country_name", "year", "land_cover_category", "element", "unit"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 7. temp_change_annual
#    Source: bronze.fao_temp_change_data_noflag | Keys: country_name, year, months, element
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.temp_change_annual_raw",
    comment="Intermediate streaming table for temp_change_annual (SDP).",
)
def temp_change_annual_raw():
    sources = {"fao_data": spark.readStream.table("climate_energy_demand.bronze.fao_temp_change_data_noflag")}
    df = process_temp_change(sources, {"start_year": 2010})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.temp_change_annual",
    comment="Monthly and annual temperature change anomalies from FAO (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.temp_change_annual",
    source="climate_energy_demand.silver_sdp.temp_change_annual_raw",
    keys=["country_name", "year", "months", "element"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 8. dim_stations
#    Source: bronze.noaa_gsod | Keys: station_id
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.dim_stations_raw",
    comment="Intermediate streaming table for dim_stations (SDP).",
)
def dim_stations_raw():
    sources = {"noaa_raw": spark.readStream.table("climate_energy_demand.bronze.noaa_gsod")}
    # skip_dedup: the transform's window-based latest-per-station selection is
    # not streamable and is redundant here — apply_changes below deduplicates
    # by station_id with _loaded_at as the sequence.
    df = create_dim_stations(sources, {"h3_resolution": 6, "skip_dedup": True})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.dim_stations",
    comment="Master lookup for weather stations mapped to countries (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.dim_stations",
    source="climate_energy_demand.silver_sdp.dim_stations_raw",
    keys=["station_id"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)

# -----------------------------------------------------------------------------
# 9. dim_date  (no streaming source — system-generated)
#    Keys: date
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.dim_date",
    comment="Master temporal dimension (2010-2040) with weekend and EU seasonality flags (SDP).",
)
def dim_date():
    return create_dim_date({}, {"start_date": "2010-01-01", "end_date": "2040-12-31"})

# -----------------------------------------------------------------------------
# 10. dim_locations
#     Sources: bronze.fao_land_use_area_codes, bronze.owid_energy | Keys: iso_code
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.dim_locations_raw",
    comment="Intermediate streaming table for dim_locations (SDP).",
)
def dim_locations_raw():
    sources = {
        "fao_areas": spark.readStream.table("climate_energy_demand.bronze.fao_land_use_area_codes"),
        "owid_energy": spark.readStream.table("climate_energy_demand.bronze.owid_energy"),
    }
    df = create_dim_locations(sources, {})
    return df.withColumn("_loaded_at", current_timestamp())

dlt.create_streaming_table(
    name="climate_energy_demand.silver_sdp.dim_locations",
    comment="Master country lookup mapping names to ISO-3166 Alpha-3 codes (SDP).",
)
dlt.apply_changes(
    target="climate_energy_demand.silver_sdp.dim_locations",
    source="climate_energy_demand.silver_sdp.dim_locations_raw",
    keys=["iso_code"],
    sequence_by=col("_loaded_at"),
    stored_as_scd_type=1,
)