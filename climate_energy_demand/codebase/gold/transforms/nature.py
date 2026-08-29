from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_forest_resilience(spark, sources: dict, target_table: str):
    """
    Transforms Silver forest and weather data into the Gold Forestry Fact table.
    Logic: Aggregates daily H3 weather into annual stress metrics (Heat/Drought).
    """
    
    # 1. Load Silver Sources
    df_forest = spark.read.table(sources['forest_source'])
    df_weather = spark.read.table(sources['weather_source'])
    df_h3 = spark.read.table(sources['h3_dim_source'])
    
    rows_read = df_weather.count()

    # 2. Identify Extreme Heat Thresholds (WMO 95th Percentile per H3 cell)
    # This ensures "Extreme Heat" is relative to the specific forest's climate.
    h3_window = Window.partitionBy("h3_cell_index")
    df_heat_thresholds = df_weather.withColumn(
        "temp_95p", F.expr("percentile_approx(temp_max, 0.95)").over(h3_window)
    ).select("h3_cell_index", "temp_95p").distinct()

    # 3. Aggregate Daily Weather to Annual Stress Metrics
    # - extreme_heat_days: Days exceeding the local 95th percentile.
    # - max_consecutive_dry_days: The longest streak of zero precipitation (Drought Proxy).
    
    # Logic for Consecutive Dry Days (Gaps & Islands pattern)
    dry_window = Window.partitionBy("h3_cell_index", "year").orderBy("date")
    
    df_weather_stress = df_weather.join(F.broadcast(df_heat_thresholds), "h3_cell_index") \
        .withColumn("is_dry", F.when(F.col("precipitation") == 0, 1).otherwise(0)) \
        .withColumn("dry_day_group", F.sum(F.when(F.col("is_dry") == 0, 1).otherwise(0)).over(dry_window)) \
        .filter(F.col("is_dry") == 1) \
        .groupBy("h3_cell_index", "year", "dry_day_group") \
        .agg(F.count("*").alias("dry_streak_length"),
             F.max(F.when(F.col("temp_max") > F.col("temp_95p"), 1).otherwise(0)).alias("heat_stress_flag"))

    # 4. Final Annual Weather Rollup
    df_weather_annual = df_weather_stress.groupBy("h3_cell_index", "year").agg(
        F.max("dry_streak_length").alias("max_consecutive_dry_days"),
        F.sum(F.when(F.col("heat_stress_flag") == 1, 1).otherwise(0)).alias("extreme_heat_days_count"),
        F.avg("temp_max").alias("annual_avg_temp_max")
    )

    # 5. Spatial Join: Forestry Carbon + Weather Stress
    # Join on H3 Cell and Year
    df_gold = df_forest.join(
        df_weather_annual,
        ["h3_cell_index", "year"],
        "inner"
    )

    # 6. Join H3 Dimension for Centroids
    df_gold = df_gold.join(
        F.broadcast(df_h3.select("h3_cell_index", "centroid_lat", "centroid_lon")),
        ["h3_cell_index"],
        "left"
    )

    # 7. Final Calculations & Aliasing
    final_df = df_gold.select(
        F.col("h3_cell_index"),
        F.col("year"),
        F.col("net_flux").alias("carbon_net_flux"),
        F.col("forest_area").alias("forest_area_ha"),
        F.col("tree_cover_loss").alias("tree_cover_loss_ha"),
        F.col("extreme_heat_days_count"),
        F.col("max_consecutive_dry_days"),
        F.col("centroid_lat"),
        F.col("centroid_lon"),
        # Calculate Sequestration Efficiency: Carbon Flux per Hectare
        (F.col("net_flux") / F.col("forest_area")).alias("sequestration_efficiency")
    )

    # 8. Write to Gold Table
    final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    rows_written = final_df.count()
    
    return rows_read, rows_written