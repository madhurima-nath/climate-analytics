from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_energy_demand(spark, sources: dict, target_table: str):
    """
    Transforms Silver weather and energy data into the Gold Energy Demand Fact table.
    Logic: Calculates temperature anomalies and demand sensitivity index (DSI).
    """
    
    # 1. Load Silver Sources
    df_weather = spark.read.table(sources['weather_source'])
    df_energy = spark.read.table(sources['energy_source'])
    df_date = spark.read.table(sources['date_dim_source'])
    
    rows_read = df_weather.count()

    # 2. Calculate Climatological Normals (10-year monthly average per country)
    # This defines what "Normal" weather looks like for each location/month.
    month_window = Window.partitionBy("iso_code", "month")
    df_normals = df_weather.withColumn(
        "historical_monthly_avg", 
        F.avg("temp_mean").over(month_window)
    ).select("iso_code", "month", "historical_monthly_avg").distinct()

    # 3. Join Normals and Calculate Anomalies
    df_enriched_weather = df_weather.join(
        F.broadcast(df_normals), ["iso_code", "month"], "left"
    ).withColumn(
        "temp_anomaly", F.col("temp_mean") - F.col("historical_monthly_avg")
    )

    # 4. Heatwave Logic (Relative Threshold: 3+ consecutive days of +5°C anomaly)
    # Senior Logic: Using Window functions to check persistence
    heat_window = Window.partitionBy("iso_code").orderBy("date")
    df_weather_final = df_enriched_weather.withColumn(
        "is_heat_spike", F.when(F.col("temp_anomaly") > 5, 1).otherwise(0)
    ).withColumn(
        "consecutive_heat_days", 
        F.sum("is_heat_spike").over(heat_window.rowsBetween(-2, 0))
    ).withColumn(
        "is_heatwave_event", F.when(F.col("consecutive_heat_days") >= 3, True).otherwise(False)
    )

    # 5. Broadcast Join: Attach Annual Energy to Daily Weather
    # Using broadcast because df_energy is small (one row per country/year)
    df_gold = df_weather_final.join(
        F.broadcast(df_energy.select("iso_code", "year", "electricity_demand", "renewables_share")),
        ["iso_code", "year"],
        "inner"
    )

    # 6. Join Date Dimension for Holidays/Weekends
    df_gold = df_gold.join(
        F.broadcast(df_date.select("date", "is_holiday", "is_weekend")),
        ["date"],
        "left"
    )

    # 7. Final Calculations & Aliasing
    final_df = df_gold.select(
        F.col("iso_code"),
        F.col("date"),
        F.col("temp_mean").alias("avg_temperature_c"),
        F.col("temp_anomaly"),
        F.col("is_heatwave_event"),
        F.col("hdd").alias("heating_degree_days"),
        F.col("cdd").alias("cooling_degree_days"),
        F.col("electricity_demand").alias("annual_baseline_twh"),
        F.col("renewables_share").alias("renewables_percentage"),
        F.col("is_holiday").alias("is_public_holiday"),
        F.col("is_weekend"),
        # Calculate Demand Sensitivity Index (DSI)
        ((F.col("hdd") + F.col("cdd")) / (F.col("electricity_demand") / 365)).alias("demand_sensitivity_index")
    )

    # 8. Write to Gold Table
    final_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    rows_written = final_df.count()
    
    return rows_read, rows_written