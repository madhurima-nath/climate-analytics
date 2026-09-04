
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

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
    
    # 4. Final Cleanup: Cast year to INT and filter for our study period
    return df_long.withColumn("year", F.col("year").cast("int")).filter("year >= 2010")