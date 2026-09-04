
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def create_dim_h3_grid(sources: dict, params: dict) -> DataFrame:
    """
    Creates a master spatial grid using H3 Indexing (Resolution 6).
    Classifies cells based on Land Type (Forest, Peatland, Urban).
    """
    res = params.get("h3_resolution", 6)
    peatlands = sources["peatlands"]
    
    # Logic: Convert lat/lon points from Bronze into H3 cells
    # Note: If using polygons, we would use Mosaic's grid_polyfill
    return peatlands.select(
        F.expr(f"h3_latlngtocell(latitude, longitude, {res})").alias("h3_cell"),
        F.lit("Peatland").alias("land_type"),
        "country"
    ).distinct()

def process_carbon_flux_spatial(sources: dict, params: dict) -> DataFrame:
    """
    Maps GFW Carbon Flux (Source/Sink) to the H3 grid.
    """
    res = params.get("h3_resolution", 6)
    flux_raw = sources["flux_raw"]
    
    return flux_raw.select(
        F.expr(f"h3_latlngtocell(latitude, longitude, {res})").alias("h3_cell"),
        F.col("year").cast("int"),
        F.col("net_flux_co2e_ha").alias("flux_value"),
        # Determine if the cell is a Sink or Source
        F.when(F.col("net_flux_co2e_ha") < 0, "Sink").otherwise("Source").alias("flux_type")
    )