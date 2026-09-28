
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ============================================================================
# SILVER LAYER TRANSFORMATIONS
# ============================================================================

def process_forest_inventory(sources: dict, params: dict) -> DataFrame:
    """
    Consolidates FAO Land Use and Carbon data.
    Unpivots 'Wide' year columns into 'Long' format.
    """
    data = sources["fao_data"]
    items = sources["fao_items"]
    
    # 1. Join with Metadata to get human-readable item names (e.g., 'Forest land')
    # Qualify columns to avoid ambiguous references (both tables have 'item')
    df = data.alias("data").join(
        items.alias("items"),
        on="item_code",
        how="inner"
    )
    
    # 2. Identify Year Columns (those starting with 'y' like y2010, y2011)
    year_cols = [c for c in df.columns if c.startswith("y") and c[1:].isdigit()]
    
    # 3. Unpivot (Melt) the Year columns into a single 'year' and 'value' column
    # We use stack() for high performance in Spark
    stack_expr = f"stack({len(year_cols)}, " + ", ".join([f"'{c[1:]}', {c}" for c in year_cols]) + ") as (year, value)"
    
    df_long = df.select(
        F.col("area").alias("country_name"),
        F.col("items.item").alias("land_use_category"),  # e.g., "Forest land", "Cropland"
        "unit",
        F.expr(stack_expr)
    )
    
    # 4. Final Cleanup: Cast year to INT, filter for our study period, and deduplicate
    return df_long.withColumn("year", F.col("year").cast("int")) \
                  .filter("year >= 2010") \
                  .dropDuplicates(["country_name", "year", "land_use_category", "unit"])


def process_land_cover(sources: dict, params: dict) -> DataFrame:
    """
    Unpivots FAO Land Cover data from wide year columns to long format.
    Preserves the satellite source (element) as a dimension for cross-source comparison.
    """
    data = sources["fao_data"]

    # 1. Identify Year Columns (those starting with 'y' like y1992, y2010)
    year_cols = [c for c in data.columns if c.startswith("y") and c[1:].isdigit()]

    # 2. Unpivot (Melt) the Year columns into a single 'year' and 'value' column
    stack_expr = f"stack({len(year_cols)}, " + ", ".join([f"'{c[1:]}', {c}" for c in year_cols]) + ") as (year, value)"

    df_long = data.select(
        F.col("area").alias("country_name"),
        F.col("item").alias("land_cover_category"),
        "element",   # Satellite source: CCI_LC, CGLS, MODIS, WorldCover
        "unit",
        F.expr(stack_expr)
    )

    # 3. Final Cleanup: Cast year to INT, filter for study period, deduplicate
    return df_long.withColumn("year", F.col("year").cast("int")) \
                  .filter(f"year >= {params.get('start_year', 2010)}") \
                  .dropDuplicates(["country_name", "year", "land_cover_category", "element", "unit"])


def process_temp_change(sources: dict, params: dict) -> DataFrame:
    """
    Unpivots FAO Temperature Change data from wide year columns to long format.
    Fixes character encoding mojibake caused by reading UTF-8 CSVs with
    ISO-8859-1 encoding in the bronze layer:
      - Degree symbol (°, UTF-8 0xC2 0xB0) was read as 'Â°' → fixed to '°'
      - En-dash (–, UTF-8 0xE2 0x80 0x93) was read as 'â' + control chars → fixed to '-'
    """
    data = sources["fao_data"]

    # 1. Fix encoding mojibake from bronze ISO-8859-1 ingestion
    data = (data
        .withColumn("unit", F.regexp_replace(F.col("unit"), "Â°", "°"))
        .withColumn("months", F.regexp_replace(F.col("months"), "â", "-"))
        .withColumn("months", F.regexp_replace(F.col("months"), "[\\x00-\\x1F]", ""))
    )

    # 2. Identify Year Columns (those starting with 'y' like y1961, y2010)
    year_cols = [c for c in data.columns if c.startswith("y") and c[1:].isdigit()]

    # 3. Unpivot (Melt) the Year columns
    stack_expr = f"stack({len(year_cols)}, " + ", ".join([f"'{c[1:]}', {c}" for c in year_cols]) + ") as (year, value)"

    df_long = data.select(
        F.col("area").alias("country_name"),
        "months",
        "element",
        "unit",
        F.expr(stack_expr)
    )

    # 4. Final Cleanup: Cast year to INT, filter for study period, deduplicate
    return df_long.withColumn("year", F.col("year").cast("int")) \
                  .filter(f"year >= {params.get('start_year', 2010)}") \
                  .dropDuplicates(["country_name", "year", "months", "element"])


# ============================================================================
# GOLD LAYER TRANSFORMATIONS
# ============================================================================

def process_land_cover_gold(sources: dict, params: dict) -> DataFrame:
    """
    Gold layer transform for land cover data.
    Joins silver land cover with dim_locations for ISO code resolution.
    Converts area from 1000 ha to ha.
    """
    land_cover = sources["land_cover_source"]
    location_dim = sources["location_dim_source"]

    # Resolve ISO codes via dim_locations
    result = land_cover.join(
        F.broadcast(location_dim.select("country_name", "iso_code")),
        on="country_name",
        how="inner"
    )

    # Drop rows where source value is null (FAO data unavailable for this combination)
    result = result.filter(F.col("value").isNotNull())

    # Convert 1000 ha to ha
    result = result.withColumn("area_ha", F.col("value") * 1000)

    return result.select(
        "iso_code",
        "country_name",
        "year",
        "land_cover_category",
        "element",
        "area_ha"
    )


def process_temp_change_gold(sources: dict, params: dict) -> DataFrame:
    """
    Gold layer transform for temperature change data.
    Joins silver temp change with dim_locations for ISO code resolution.
    Adds a boolean warming flag for easy filtering.
    """
    temp_change = sources["temp_change_source"]
    location_dim = sources["location_dim_source"]

    # Resolve ISO codes via dim_locations
    result = temp_change.join(
        F.broadcast(location_dim.select("country_name", "iso_code")),
        on="country_name",
        how="inner"
    )

    # Add warming flag: TRUE when element is 'Temperature change' and value is positive
    result = result.withColumn(
        "is_warming",
        F.when(
            (F.col("element") == "Temperature change") & (F.col("value") > 0),
            True
        ).otherwise(False)
    )

    return result.select(
        "iso_code",
        "country_name",
        "year",
        "months",
        "element",
        "unit",
        "value",
        "is_warming"
    )


def process_forest_resilience(sources: dict, params: dict) -> DataFrame:
    """
    Creates the Gold layer fact table: fct_forest_resilience_annual.

    Maps forest carbon health against annual climate stressors at country-year
    grain. Combines FAO forest inventory data (area + biomass) with Open-Meteo
    weather aggregates, using dim_locations to resolve ISO codes.

    Grain: 1 row per (iso_code, year)
    """
    forest_df = sources["forest_source"]
    weather_df = sources["weather_source"]
    location_dim = sources["location_dim_source"]

    # 1. Extract forest area (1000 ha → ha) and carbon stock (million t) from inventory
    forest_area = forest_df.filter(
        (F.col("land_use_category") == "Forest land") &
        (F.col("unit") == "1000 ha")
    ).select(
        "country_name", "year",
        (F.col("value") * 1000).alias("forest_area_ha")
    )

    forest_carbon = forest_df.filter(
        (F.col("land_use_category") == "Forest land") &
        (F.col("unit") == "million t")
    ).select(
        "country_name", "year",
        F.col("value").alias("forest_carbon_mt")
    )

    # Outer join ensures we keep country-years that have area or carbon or both
    forest_combined = forest_area.join(
        forest_carbon,
        on=["country_name", "year"],
        how="outer"
    )

    # 2. Aggregate daily weather to annual country-level climate stressors
    weather_annual = weather_df.withColumn(
        "date", F.col("date").cast("date")
    ).withColumn(
        "year", F.year(F.col("date"))
    ).groupBy("country", "year").agg(
        # Count days exceeding 30°C (WMO extreme heat threshold)
        F.sum(
            F.when(F.col("temp_max_c") > 30.0, 1).otherwise(0)
        ).alias("extreme_heat_days_count"),
        F.avg("temp_mean_c").alias("avg_temp_c")
    ).withColumnRenamed("country", "country_name")

    # 3. Join forest inventory with weather stressors on country + year
    joined = forest_combined.join(
        weather_annual,
        on=["country_name", "year"],
        how="inner"
    )

    # 4. Resolve ISO codes via dim_locations
    joined = joined.join(
        F.broadcast(location_dim.select("country_name", "iso_code")),
        on="country_name",
        how="inner"
    )

    # 4b. Add satellite-observed forest area from FAO Land Cover (WorldCover source)
    land_cover = sources.get("land_cover_source")
    if land_cover is not None:
        satellite_forest = land_cover.filter(
            (F.col("land_cover_category") == "Tree-covered areas") &
            (F.col("element") == "Area from WorldCover")
        ).select(
            "country_name", "year",
            (F.col("value") * 1000).alias("satellite_forest_area_ha")
        )
        joined = joined.join(satellite_forest, on=["country_name", "year"], how="left")
    else:
        joined = joined.withColumn("satellite_forest_area_ha", F.lit(None).cast("double"))

    # 4c. Calculate discrepancy between satellite and administrative forest area
    joined = joined.withColumn(
        "forest_area_discrepancy_pct",
        F.when(
            F.col("forest_area_ha").isNotNull() & (F.col("forest_area_ha") > 0) & F.col("satellite_forest_area_ha").isNotNull(),
            F.round(((F.col("satellite_forest_area_ha") - F.col("forest_area_ha")) / F.col("forest_area_ha")) * 100, 2)
        ).otherwise(None)
    )

    # 5. Calculate carbon density — carbon stock per hectare (efficiency proxy)
    result = joined.withColumn(
        "carbon_density",
        F.when(
            F.col("forest_area_ha") > 0,
            F.col("forest_carbon_mt") / F.col("forest_area_ha")
        ).otherwise(None)
    )

    # 6. Final projection aligned with YAML config schema
    return result.select(
        "iso_code",
        "country_name",
        "year",
        "forest_area_ha",
        "forest_carbon_mt",
        "carbon_density",
        "extreme_heat_days_count",
        "avg_temp_c",
        "satellite_forest_area_ha",
        "forest_area_discrepancy_pct"
    )