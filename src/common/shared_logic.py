# Location: src/transforms/common_transforms.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def relational_normalisation(df: DataFrame, id_columns: list) -> DataFrame:
    """
    Restructures 'Wide' tables (years as columns) into 'Long' format.
    """
    all_cols = df.columns
    # Logic remains solid: detects 'y1990', 'y2020', etc.
    year_cols = [c for c in all_cols if c.startswith('y') and c[1:].isdigit()]
    
    if not year_cols:
        return df 

    # .unpivot is standard in Spark 4.0+ (2026)
    return df.unpivot(
        ids=id_columns,
        values=year_cols,
        variableColumnName="observation_year",
        valueColumnName="metric_value"
    ).withColumn(
        "observation_year", 
        F.substring(F.col("observation_year"), 2, 4).cast("int")
    )

def calculate_thermal_stress(df: DataFrame, temp_col: str) -> DataFrame:
    """Calculates Heating (Base 15C) and Cooling (Base 25C) degree days."""
    return df.withColumn("heating_degree_days", F.greatest(F.lit(0), F.lit(15) - F.col(temp_col))) \
             .withColumn("cooling_degree_days", F.greatest(F.lit(0), F.col(temp_col) - F.lit(25)))