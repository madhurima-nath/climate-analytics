
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# ============================================================================
# GOLD LAYER TRANSFORMATIONS
# ============================================================================

def process_fidelity_audit(sources: dict, params: dict) -> DataFrame:
    """
    Creates the Gold layer fact table: fct_ground_truth_verification_daily.

    Compares physical weather station observations (NOAA GSOD) against
    reanalysis model data (Open-Meteo) to calculate model precision,
    bias, and accuracy metrics.

    Note: weather_historical is at country grain; observations are at station
    grain. Comparison joins on country + date, so all stations within a
    country are compared against the same country-level modeled mean.

    Grain: 1 row per (station_id, date)
    """
    physical_df = sources["physical_observations"]   # NOAA GSOD
    modeled_df = sources["modeled_data"]               # Open-Meteo
    station_dim = sources["station_dim"]

    # 1. Select and rename observed temperature from physical stations
    observed_clean = physical_df.select(
        "country", "station_id", "date",
        F.col("temp_mean_c").alias("observed_avg_temp")
    ).dropDuplicates(["station_id", "date"])

    # 2. Cast modeled date from string to date and rename temperature column
    modeled_clean = modeled_df.withColumn(
        "date", F.col("date").cast("date")
    ).select(
        "country", "date",
        F.col("temp_mean_c").alias("modeled_avg_temp")
    ).dropDuplicates(["country", "date"])

    # 3. Join physical observations with modeled data on country + date
    joined = observed_clean.join(
        modeled_clean,
        on=["country", "date"],
        how="inner"
    )

    # 4. Enrich with station metadata from dim_stations
    joined = joined.join(
        station_dim.select("station_id", "station_name"),
        on="station_id",
        how="left"
    )

    # 5. Calculate fidelity metrics
    #    Absolute Error: |Modeled - Observed|
    #    Bias Error: Modeled - Observed (positive = model overestimates heat)
    #    Tolerance: EU/WMO acceptable threshold for regional climate modeling (2.0°C)
    result = joined.withColumn(
        "absolute_error",
        F.abs(F.col("modeled_avg_temp") - F.col("observed_avg_temp"))
    ).withColumn(
        "bias_error",
        F.col("modeled_avg_temp") - F.col("observed_avg_temp")
    ).withColumn(
        "is_within_tolerance",
        F.when(F.col("absolute_error") <= 2.0, True).otherwise(False)
    )

    # 6. Filter out records where physical sensors reported NULL
    #    to prevent skewing accuracy scores
    result = result.filter(
        F.col("observed_avg_temp").isNotNull() &
        F.col("modeled_avg_temp").isNotNull()
    )

    # 7. Final projection aligned with YAML config schema
    return result.select(
        "station_id",
        "date",
        "observed_avg_temp",
        "modeled_avg_temp",
        "absolute_error",
        "bias_error",
        "is_within_tolerance",
        "station_name",
        "country"
    )