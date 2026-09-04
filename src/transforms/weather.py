
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

def process_weather_historical(sources: dict, params: dict) -> DataFrame:
    """
    Standardizes Open-Meteo historical data.
    - Removes 7-day ingestion overlaps
    - Fills gaps (max 3 days)
    - Calculates HDD (15C) and CDD (25C)
    """
    df = sources["raw_weather"]
    hdd_base = params.get("hdd_base", 15)
    cdd_base = params.get("cdd_base", 25)

    # 1. Deduplicate: Keep latest 'ingested_at' for every country/date
    window_spec = Window.partitionBy("country", "date").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1").drop("rn")

    # 2. Persistence-Based Gap Filling (Forward Fill max 3 days)
    # We use a window to look back at the last known valid temperature
    ffill_window = Window.partitionBy("country").orderBy("date").rowsBetween(-3, 0)
    
    df = df.withColumn("temp_max_c", F.last("temperature_2m_max", True).over(ffill_window))
    df = df.withColumn("temp_min_c", F.last("temperature_2m_min", True).over(ffill_window))

    # 3. Calculate Mean and Degree Days
    df = df.withColumn("temp_mean_c", (F.col("temp_max_c") + F.col("temp_min_c")) / 2)
    
    # HDD = Max(0, 15 - Mean) | CDD = Max(0, Mean - 25)
    df = df.withColumn("hdd_15", F.greatest(F.lit(0), hdd_base - F.col("temp_mean_c")))
    df = df.withColumn("cdd_25", F.greatest(F.lit(0), F.col("temp_mean_c") - cdd_base))

    return df.select("country", "date", "temp_max_c", "temp_min_c", "temp_mean_c", "hdd_15", "cdd_25")

def process_weather_observations(sources: dict) -> DataFrame:
    """
    Standardizes NOAA GSOD data from Imperial to Metric.
    """
    df = sources["noaa_raw"]

    # Conversions: (F - 32) * 5/9 = C  |  Inches * 25.4 = mm
    return df.select(
        "country",
        "station_id",
        F.to_date("date").alias("date"),
        F.round((F.col("temp") - 32) * 5/9, 2).alias("temp_mean_c"),
        F.round((F.col("max") - 32) * 5/9, 2).alias("temp_max_c"),
        F.round((F.col("min") - 32) * 5/9, 2).alias("temp_min_c"),
        F.round(F.col("prcp") * 25.4, 2).alias("precip_mm")
    )

def create_dim_stations(sources: dict, params: dict) -> DataFrame:
    """
    Creates a master lookup for weather stations from NOAA GSOD data.
    Maps station IDs to coordinates, H3 hexagons, and countries.
    """
    df = sources["noaa_raw"]
    h3_resolution = params.get("h3_resolution", 6)
    
    # Get unique stations with their latest metadata
    window_spec = Window.partitionBy("station_id").orderBy(F.col("ingested_at").desc())
    stations = df.withColumn("rn", F.row_number().over(window_spec)) \
                 .filter("rn = 1") \
                 .drop("rn")
    
    # Assign H3 hexagon
    stations = stations.withColumn(
        "h3_index",
        F.expr(f"h3_latlngtocell(latitude, longitude, {h3_resolution})")
    )
    
    return stations.select(
        "station_id",
        "country",
        F.round("latitude", 4).alias("latitude"),
        F.round("longitude", 4).alias("longitude"),
        "h3_index"
    )

def process_weather_projections(sources: dict, params: dict) -> DataFrame:
    """
    Processes CMIP6 climate projections for future scenarios.
    Calculates HDD/CDD metrics for energy demand forecasting.
    """
    df = sources["raw_weather"]
    hdd_base = params.get("hdd_base", 15)
    cdd_base = params.get("cdd_base", 25)
    h3_resolution = params.get("h3_resolution", 6)
    
    # Calculate mean temperature and degree days
    df = df.withColumn("temp_mean_c", (F.col("temperature_2m_max") + F.col("temperature_2m_min")) / 2)
    
    # HDD = Max(0, base - Mean) | CDD = Max(0, Mean - base)
    df = df.withColumn("hdd_15", F.greatest(F.lit(0), hdd_base - F.col("temp_mean_c")))
    df = df.withColumn("cdd_25", F.greatest(F.lit(0), F.col("temp_mean_c") - cdd_base))
    
    # Assign H3 hexagon for spatial aggregation
    df = df.withColumn(
        "h3_index",
        F.expr(f"h3_latlngtocell(latitude, longitude, {h3_resolution})")
    )
    
    return df.select(
        "model",
        "country",
        F.to_date("date").alias("date"),
        F.round("latitude", 4).alias("latitude"),
        F.round("longitude", 4).alias("longitude"),
        "h3_index",
        F.round("temperature_2m_max", 2).alias("temp_max_c"),
        F.round("temperature_2m_min", 2).alias("temp_min_c"),
        F.round("temp_mean_c", 2).alias("temp_mean_c"),
        F.round("hdd_15", 2).alias("hdd_15"),
        F.round("cdd_25", 2).alias("cdd_25"),
        "ingested_at"
    )