
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