# Location: src/common/audit_utils.py
# Purpose: Manages the "Manual Watermarking" state (Sep 2026 Serverless Fix)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Finalised 3-level reference (Unity Catalog)
AUDIT_TABLE = "climate_energy_demand.silver.ingestion_audit"

def get_last_watermark(target_table_name: str) -> str:
    # Get the session that is ALREADY running in the notebook
    spark = SparkSession.getActiveSession()
    try:
        res = spark.table(AUDIT_TABLE) \
                   .filter(F.col("table_name") == target_table_name) \
                   .select(F.max("last_watermark")).collect()[0][0]
        return res if res else "1900-01-01 00:00:00"
    except Exception:
        # If table doesn't exist yet, return the default epoch
        return "1900-01-01 00:00:00"

def update_audit_log(table_name: str, watermark: str, count: int):
    spark = SparkSession.getActiveSession()
    # Format the SQL clearly for Serverless 4.0 engine
    spark.sql(f"""
        INSERT INTO {AUDIT_TABLE} 
        VALUES ('{table_name}', '{watermark}', {count}, current_timestamp())
    """)