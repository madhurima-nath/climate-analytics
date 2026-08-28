
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def process_energy_metrics(sources: dict) -> DataFrame:
    """
    Cleans and standardizes OWID energy data.
    Focuses on demand, generation, population, and GDP.
    """
    df = sources["owid_raw"]

    # Select core columns and ensure types are correct
    return df.select(
        F.col("iso_code"),
        F.col("country").alias("country_name"),
        F.col("year").cast("int"),
        F.col("electricity_demand").alias("demand_twh"),
        F.col("electricity_generation").alias("generation_twh"),
        F.col("population"),
        F.col("gdp")
    ).filter("year >= 2010") # Align with our weather data start date