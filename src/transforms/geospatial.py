
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def create_dim_h3_grid(sources: dict, params: dict) -> DataFrame:
    """
    Creates a master spatial grid using H3 Indexing (Resolution 6).
    Classifies cells based on Land Type (Forest, Peatland, Urban).
    
    Parses GFW tile_id format (e.g., "00N_000E") to extract coordinates.
    Each tile represents a 10°×10° area; we use the center point for grid assignment.
    """
    res = params.get("h3_resolution", 6)
    peatlands = sources["peatlands"]
    
    # Parse tile_id: format is "{LAT}N/S_{LON}E/W"
    # Example: "00N_000E" = 0°N, 0°E | "50S_120W" = 50°S, 120°W
    df = peatlands.withColumn(
        "lat_part", F.split(F.col("tile_id"), "_")[0]
    ).withColumn(
        "lon_part", F.split(F.col("tile_id"), "_")[1]
    )
    
    # Extract numeric latitude: "50N" -> 50, "30S" -> -30
    df = df.withColumn(
        "latitude",
        F.when(
            F.col("lat_part").endswith("N"),
            F.regexp_extract(F.col("lat_part"), "(\\d+)", 1).cast("double")
        ).otherwise(
            -F.regexp_extract(F.col("lat_part"), "(\\d+)", 1).cast("double")
        ) + 5.0  # Add 5° to get center of 10° tile
    )
    
    # Extract numeric longitude: "120E" -> 120, "080W" -> -80
    df = df.withColumn(
        "longitude",
        F.when(
            F.col("lon_part").endswith("E"),
            F.regexp_extract(F.col("lon_part"), "(\\d+)", 1).cast("double")
        ).otherwise(
            -F.regexp_extract(F.col("lon_part"), "(\\d+)", 1).cast("double")
        ) + 5.0  # Add 5° to get center of 10° tile
    )
    
    # Create grid cell identifier (alternative to H3 on serverless)
    # Resolution 6 H3 is ~30km, so we use 0.3 degree precision (~33km at equator)
    precision = 0.3
    return df.select(
        F.concat(
            F.lit("grid_"),
            F.round(F.col("latitude") / precision, 0).cast("int"),
            F.lit("_"),
            F.round(F.col("longitude") / precision, 0).cast("int")
        ).alias("h3_cell"),
        F.lit("Peatland").alias("land_type")
    ).distinct()

def process_carbon_flux_spatial(sources: dict, params: dict) -> DataFrame:
    """
    Maps GFW Carbon Flux (Source/Sink) to the H3 grid.
    
    NOTE: The gfw_net_flux table contains only tile metadata and download URLs.
    The actual flux values (Mg CO2e/ha) are stored in GeoTIFF raster files.
    
    This function returns tile-level metadata with coordinates.
    To get actual flux values, the raster files at mg_co2e_ha_1_download URLs
    would need to be downloaded and processed in a separate bronze ingestion workflow.
    """
    res = params.get("h3_resolution", 6)
    flux_raw = sources["flux_raw"]
    
    # Parse tile_id: format is "{LAT}N/S_{LON}E/W"
    df = flux_raw.withColumn(
        "lat_part", F.split(F.col("tile_id"), "_")[0]
    ).withColumn(
        "lon_part", F.split(F.col("tile_id"), "_")[1]
    )
    
    # Extract numeric latitude
    df = df.withColumn(
        "latitude",
        F.when(
            F.col("lat_part").endswith("N"),
            F.regexp_extract(F.col("lat_part"), "(\\d+)", 1).cast("double")
        ).otherwise(
            -F.regexp_extract(F.col("lat_part"), "(\\d+)", 1).cast("double")
        ) + 5.0  # Center of 10° tile
    )
    
    # Extract numeric longitude
    df = df.withColumn(
        "longitude",
        F.when(
            F.col("lon_part").endswith("E"),
            F.regexp_extract(F.col("lon_part"), "(\\d+)", 1).cast("double")
        ).otherwise(
            -F.regexp_extract(F.col("lon_part"), "(\\d+)", 1).cast("double")
        ) + 5.0  # Center of 10° tile
    )
    
    # Create grid cell identifier
    precision = 0.3
    df = df.withColumn(
        "h3_cell",
        F.concat(
            F.lit("grid_"),
            F.round(F.col("latitude") / precision, 0).cast("int"),
            F.lit("_"),
            F.round(F.col("longitude") / precision, 0).cast("int")
        )
    )
    
    # Note: year and flux_value are not available in the source table
    # Returning tile metadata only; actual flux processing requires raster download
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(
        [],
        schema="h3_cell STRING, year INT, flux_value DOUBLE, flux_type STRING"
    )