import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

class GoldLogger:
    """
    Handles transactional logging and observability for the Gold Layer.
    Persists execution metadata to the 'gold.audit_logs' Delta table.
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "climate_energy_demand", schema: str = "gold"):
        self.spark = spark
        self.audit_table = f"{catalog}.{schema}.audit_logs"
        self.run_id = str(uuid.uuid4())
        self._ensure_audit_table()

    def _ensure_audit_table(self):
        """Creates the audit table if it does not exist in Unity Catalog."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.audit_table} (
                run_id STRING,
                target_table STRING,
                status STRING,
                rows_read LONG,
                rows_written LONG,
                execution_start_ts TIMESTAMP,
                execution_end_ts TIMESTAMP,
                duration_seconds DOUBLE,
                error_message STRING
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)

    def log_step(self, target_table: str, status: str, start_ts: datetime, 
                 rows_read: int = 0, rows_written: int = 0, error_message: str = None):
        """
        Records the outcome of a transformation step.
        """
        end_ts = datetime.now()
        duration = (end_ts - start_ts).total_seconds()

        # Define the schema for the log record
        log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("status", StringType(), False),
            StructField("rows_read", LongType(), True),
            StructField("rows_written", LongType(), True),
            StructField("execution_start_ts", TimestampType(), False),
            StructField("execution_end_ts", TimestampType(), False),
            StructField("duration_seconds", DoubleType(), True),
            StructField("error_message", StringType(), True)
        ])

        # Prepare the log data
        log_data = [(
            self.run_id,
            target_table,
            status,
            rows_read,
            rows_written,
            start_ts,
            end_ts,
            duration,
            error_message
        )]

        # Create DataFrame and append to audit table
        log_df = self.spark.createDataFrame(log_data, schema=log_schema)
        log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)
        
        print(f"[AUDIT] Table: {target_table} | Status: {status} | Duration: {duration}s")