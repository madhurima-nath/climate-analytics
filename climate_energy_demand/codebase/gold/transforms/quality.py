from pyspark.sql import functions as F

def process_fidelity_audit(spark, sources: dict, target_table: str):
    """
    Transforms Silver physical observations and modeled data into the Gold Quality table.
    Logic: Calculates row-level error metrics and tolerance flags for model validation.
    """
    
    # 1. Load Silver Sources
    df_physical = spark.read.table(sources['physical_observations']) # NOAA GSOD
    df_modeled = spark.read.table(sources['modeled_data'])           # Open-Meteo
    df_stations = spark.read.table(sources['station_dim'])
    
    rows_read = df_physical.count()

    # 2. Prepare Modeled Data for Join
    # Open-Meteo data in Silver has a 'nearest_station_id' mapping from the Haversine step.
    df_modeled_prep = df_modeled.select(
        F.col("nearest_station_id").alias("station_id"),
        F.col("date"),
        F.col("temp_mean").alias("modeled_avg_temp")
    )

    # 3. Inner Join: Physical vs Modeled
    # We use an Inner Join to ensure we only compare days where BOTH sources have data.
    df_comparison = df_physical.select(
        F.col("station_id"),
        F.col("date"),
        F.col("temp_mean").alias("observed_avg_temp")
    ).join(
        df_modeled_prep,
        ["station_id", "date"],
        "inner"
    )

    # 4. Calculate Error Metrics
    # - Absolute Error: Magnitude of deviation
    # - Bias Error: Direction of deviation (Modeled - Observed)
    # - Tolerance: Senior-level benchmark (2.0°C threshold)
    df_metrics = df_comparison.withColumn(
        "absolute_error", F.abs(F.col("modeled_avg_temp") - F.col("observed_avg_temp"))
    ).withColumn(
        "bias_error", F.col("modeled_avg_temp") - F.col("observed_avg_temp")
    ).withColumn(
        "is_within_tolerance", F.when(F.col("absolute_error") <= 2.0, True).otherwise(False)
    )

    # 5. Join Station Metadata
    # Adds elevation and country code to analyze if error is correlated with geography.
    df_gold = df_metrics.join(
        F.broadcast(df_stations.select("station_id", "elevation", "country")),
        ["station_id"],
        "left"
    )

    # 6. Final Select and Aliasing
    final_df = df_gold.select(
        F.col("station_id"),
        F.col("date"),
        F.col("observed_avg_temp"),
        F.col("modeled_avg_temp"),
        F.col("absolute_error"),
        F.col("bias_error"),
        F.col("is_within_tolerance"),
        F.col("elevation").alias("station_elevation_m"),
        F.col("country").alias("country_code")
    )

    # 7. Data Quality Filter
    # Remove records with null observations to ensure the "Trust Score" is scientifically valid.
    final_df = final_df.filter(F.col("observed_avg_temp").isNotNull())

    # 8. Write to Gold Table
    final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    rows_written = final_df.count()
    
    return rows_read, rows_written