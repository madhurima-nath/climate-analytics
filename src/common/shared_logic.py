import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def relational_normalisation(df: DataFrame, id_columns: list) -> DataFrame:
    """
    Restructures 'Wide' tables (years as columns) into 'Long' format.
    Logic: Identifies columns starting with 'y' followed by 4 digits.
    """
    all_cols = df.columns
    year_cols = [c for c in all_cols if c.startswith('y') and c[1:].isdigit()]
    
    if not year_cols:
        return df # Return unchanged if no year columns exist

    return df.unpivot(
        ids=id_columns,
        values=year_cols,
        variableColumnName="observation_year",
        valueColumnName="metric_value"
    ).withColumn(
        "observation_year", 
        F.substring(F.col("observation_year"), 2, 4).cast("int")
    )

def geospatial_indexing(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
    """Maps coordinates to Uber H3 Hexagons at Resolution 6."""
    # Assuming the H3 library is pre-installed in the 2026 runtime
    import h3
    h3_udf = F.udf(lambda lat, lon: h3.geo_to_h3(lat, lon, 6))
    return df.withColumn("h3_index_res6", h3_udf(F.col(lat_col), F.col(lon_col)))

def calculate_thermal_stress(df: DataFrame, temp_col: str) -> DataFrame:
    """Calculates Heating (Base 15C) and Cooling (Base 25C) degree days."""
    return df.withColumn("heating_degree_days", F.greatest(F.lit(0), F.lit(15) - F.col(temp_col))) \
             .withColumn("cooling_degree_days", F.greatest(F.lit(0), F.col(temp_col) - F.lit(25)))