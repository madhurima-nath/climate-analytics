
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def create_dim_locations(sources: dict) -> DataFrame:
    """
    Joins FAO and OWID metadata to create a single master list of countries.
    This ensures every country has a standard ISO Alpha-3 code.
    """
    fao_areas = sources["fao_areas"]
    owid_energy = sources["owid_energy"]

    # Get unique names and ISO codes from OWID
    owid_clean = owid_energy.select(
        F.col("country").alias("country_name"),
        F.col("iso_code")
    ).filter("iso_code IS NOT NULL").distinct()

    # Get FAO codes from FAO area codes file
    fao_clean = fao_areas.select(
        F.col("area_code").alias("fao_code"),
        F.col("area").alias("country_name")
    ).distinct()

    # Join them together so we have one 'Master' mapping table
    return owid_clean.join(fao_clean, on="country_name", how="inner")

def create_dim_date(params: dict) -> DataFrame:
    """
    Generates a calendar from 2010 to 2050 with weekend and season flags.
    """
    start = params.get("start_date", "2010-01-01")
    end = params.get("end_date", "2050-12-31")

    # Spark SQL logic to create a range of dates
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) as date")

    return df.select(
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        # Weekend: 1=Sun, 7=Sat
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        # EU Seasonality
        F.when(F.month("date").isin(12, 1, 2), "Winter")
         .when(F.month("date").isin(3, 4, 5), "Spring")
         .when(F.month("date").isin(6, 7, 8), "Summer")
         .otherwise("Autumn").alias("season")
    )