
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ============================================================================
# SILVER LAYER TRANSFORMATIONS
# ============================================================================

def process_energy_metrics(sources: dict, params: dict) -> DataFrame:
    """
    Cleans and standardizes OWID energy data.
    Focuses on demand, generation, population, and GDP.
    """
    df = sources["owid_raw"]

    # Select core columns and ensure types are correct
    # Filter out records with null iso_code (primary key) and duplicates
    return df.select(
        F.col("iso_code"),
        F.col("country").alias("country_name"),
        F.col("year").cast("int"),
        F.col("electricity_demand").alias("demand_twh"),
        F.col("electricity_generation").alias("generation_twh"),
        F.col("population"),
        F.col("gdp")
    ).filter(
        (F.col("iso_code").isNotNull()) &  # Remove null primary keys
        (F.col("year") >= 2010)  # Align with our weather data start date
    ).dropDuplicates(["iso_code", "year"])  # Deduplicate on primary key


# ============================================================================
# GOLD LAYER TRANSFORMATIONS
# ============================================================================

def process_energy_demand(sources: dict, params: dict) -> DataFrame:
    """
    Creates the Gold layer fact table: fct_energy_demand_daily.

    Joins daily weather stressors (silver.weather_historical) with national
    energy baselines (silver.energy_metrics), using gold.dim_locations to resolve
    ISO codes and get climate-zone-specific base temperatures for dynamic HDD/CDD.

    Calculates:
      - Dynamic HDD/CDD: Uses Köppen-based base temps per climate zone (not hardcoded 15/25)
      - Demand Sensitivity Index (DSI): thermal stress normalized by daily grid capacity
      - Temperature Anomaly: departure from local 10-year monthly mean
      - Heatwave Event Flag: EU/WMO relative anomaly threshold (>5°C)

    Grain: 1 row per (iso_code, date)
    """
    weather_df = sources["weather_source"]
    energy_df = sources["energy_source"]
    date_dim = sources["date_dim_source"]
    location_dim = sources["location_dim_source"]

    # 1. Resolve ISO codes AND get dynamic base temps from gold.dim_locations
    #    Köppen-based base temps enable climate-appropriate HDD/CDD calculation.
    #    Countries with insufficient data fall back to 15°C/25°C defaults.
    location_lookup = location_dim.select(
        F.col("country_name").alias("country"),
        "iso_code",
        F.coalesce(F.col("heating_base_temp"), F.lit(15)).alias("heating_base_temp"),
        F.coalesce(F.col("cooling_base_temp"), F.lit(25)).alias("cooling_base_temp"),
    )
    weather_enriched = weather_df.join(
        F.broadcast(location_lookup),
        on="country",
        how="inner"
    )

    # 2. Normalize date type (silver stores as string) and extract year for annual join
    weather_enriched = weather_enriched.withColumn(
        "date", F.col("date").cast("date")
    ).withColumn(
        "year", F.year(F.col("date"))
    )

    # 3. Broadcast join: small annual energy table onto daily weather records
    joined = weather_enriched.join(
        F.broadcast(energy_df),
        on=["iso_code", "year"],
        how="inner"
    )

    # 4. Add calendar attributes from dim_date (weekend flag)
    joined = joined.join(
        date_dim.select("date", "is_weekend"),
        on="date",
        how="left"
    )

    # 5. Dynamic HDD/CDD using climate-zone-specific base temperatures
    #    HDD = Max(0, heating_base - temp_mean)
    #    CDD = Max(0, temp_mean - cooling_base)
    #    Falls back to 15°C/25°C for countries with insufficient climate data.
    result = joined.withColumn(
        "heating_degree_days",
        F.greatest(F.lit(0), F.col("heating_base_temp") - F.col("temp_mean_c"))
    ).withColumn(
        "cooling_degree_days",
        F.greatest(F.lit(0), F.col("temp_mean_c") - F.col("cooling_base_temp"))
    )

    # 6. Demand Sensitivity Index = (HDD + CDD) / (annual_demand / 365)
    #    Normalizes thermal stress against average daily consumption.
    #    High values indicate weather-dependent grid volatility.
    result = result.withColumn(
        "demand_sensitivity_index",
        F.when(
            F.col("demand_twh").isNotNull() & (F.col("demand_twh") > 0),
            (F.col("heating_degree_days") + F.col("cooling_degree_days")) /
            (F.col("demand_twh") * 1000000 / 365)
        ).otherwise(None)
    )

    # 6. Temperature anomaly — departure from local 10-year monthly mean
    #    Uses EU/WMO relative anomaly standard (geographic adaptation)
    month_window = Window.partitionBy("iso_code", F.month("date")) \
                         .orderBy("date") \
                         .rowsBetween(-3650, 0)  # ~10 years back

    result = result.withColumn(
        "monthly_baseline_temp",
        F.avg("temp_mean_c").over(month_window)
    ).withColumn(
        "temp_anomaly",
        F.col("temp_mean_c") - F.col("monthly_baseline_temp")
    )

    # 7. Heatwave detection — relative anomaly > 5°C (EU/WMO standard)
    result = result.withColumn(
        "is_heatwave_event",
        F.when(F.col("temp_anomaly") > 5.0, True).otherwise(False)
    )

    # 9. Final projection aligned with YAML config schema
    return result.select(
        "iso_code",
        "date",
        F.col("temp_mean_c").alias("avg_temperature_c"),
        "temp_anomaly",
        "is_heatwave_event",
        "heating_degree_days",
        "cooling_degree_days",
        "heating_base_temp",
        "cooling_base_temp",
        F.col("demand_twh").alias("annual_baseline_twh"),
        "generation_twh",
        "demand_sensitivity_index",
        F.coalesce(F.col("is_weekend"), F.lit(False)).alias("is_weekend")
    )