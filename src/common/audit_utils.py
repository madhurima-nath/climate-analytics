# Location: src/common/audit_utils.py
# Purpose: Manages the "Manual Watermarking" state (Sep 2026 Serverless Fix)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Finalised 3-level reference (Unity Catalog)
AUDIT_TABLE = "climate_energy_demand.silver.ingestion_audit"

def get_last_watermark(target_table_name: str):
    # Get the session that is ALREADY running in the notebook
    spark = SparkSession.getActiveSession()
    from datetime import datetime
    
    try:
        res = spark.table(AUDIT_TABLE) \
                   .filter(F.col("table_name") == target_table_name) \
                   .select(F.max("last_watermark")).collect()[0][0]
        return res if res else datetime(1900, 1, 1)
    except Exception:
        # If table doesn't exist yet, return the default epoch as timestamp
        return datetime(1900, 1, 1)

def update_audit_log(table_name: str, watermark, count: int):
    spark = SparkSession.getActiveSession()
    # Use parameterized query to preserve full timestamp precision
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
    from datetime import datetime
    
    # Convert string watermark to timestamp if needed
    if isinstance(watermark, str):
        watermark = datetime.fromisoformat(watermark.replace('Z', '+00:00'))
    
    data = [(table_name, watermark, count, datetime.now())]
    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("last_watermark", TimestampType(), False),
        StructField("rows_processed", IntegerType(), False),
        StructField("processed_at", TimestampType(), False)
    ])
    
    spark.createDataFrame(data, schema).write.mode("append").saveAsTable(AUDIT_TABLE)