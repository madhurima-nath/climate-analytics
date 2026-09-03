# Location: src/common/audit_utils.py
# Purpose: Manages the "Manual Watermarking" state

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
# Finalised 3-level reference
AUDIT_TABLE = "climate_energy_demand.silver.ingestion_audit"

def get_last_watermark(target_table_name: str) -> str:
    try:
        res = spark.table(AUDIT_TABLE) \
                   .filter(F.col("table_name") == target_table_name) \
                   .select(F.max("last_watermark")).collect()[0][0]
        return res if res else "1900-01-01 00:00:00"
    except:
        return "1900-01-01 00:00:00"

def update_audit_log(table_name: str, watermark: str, count: int):
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} 
        VALUES ('{table_name}', '{watermark}', {count}, current_timestamp())
    """)