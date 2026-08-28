import os
import yaml
import importlib
from datetime import datetime
from logger import GoldLogger

class GoldOrchestrator:
    """
    Orchestrates the Gold Layer: Parses YAML configs, executes Python transforms,
    and applies semantic metadata to Delta tables.
    """
    
    def __init__(self, spark, config_dir: str = "../configs"):
        self.spark = spark
        self.config_dir = config_dir
        self.logger = GoldLogger(spark)

    def _load_config(self, table_name: str):
        """Reads a YAML configuration file."""
        config_path = os.path.join(self.config_dir, f"{table_name}.yml")
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def _apply_semantic_metadata(self, table_full_name: str, columns_config: list):
        """Applies SQL COMMENTS to columns for AI/BI discovery."""
        for col in columns_config:
            name = col.get("name")
            comment = col.get("comment")
            if name and comment:
                self.spark.sql(f"ALTER TABLE {table_full_name} ALTER COLUMN {name} SET COMMENT '{comment}'")

    def run_table(self, table_name: str):
        """
        Executes a specific Gold table transformation based on its YAML config.
        """
        start_ts = datetime.now()
        config = self._load_config(table_name)
        
        # Target details
        catalog = "climate_energy_demand"
        schema = "gold"
        target_table_name = config.get("table_name")
        full_target_path = f"{catalog}.{schema}.{target_table_name}"
        
        # Dynamic module loading
        module_path = config.get("transformation_module")  # e.g., 'transforms.energy'
        func_name = config.get("transformation_function")   # e.g., 'process_energy_demand'
        
        try:
            # 1. Import the transformation function
            module = importlib.import_module(module_path)
            transform_func = getattr(module, func_name)
            
            # 2. Execute Transformation (Passes the sources defined in YAML)
            # Returns a DataFrame or a count of rows read/written
            print(f"[EXEC] Running {func_name} for {full_target_path}...")
            rows_read, rows_written = transform_func(self.spark, config.get("sources"), full_target_path)
            
            # 3. Apply Metadata (For AI/BI Genie)
            self._apply_semantic_metadata(full_target_path, config.get("columns", []))
            
            # 4. Success Log
            self.logger.log_step(
                target_table=target_table_name,
                status="SUCCESS",
                start_ts=start_ts,
                rows_read=rows_read,
                rows_written=rows_written
            )
            
        except Exception as e:
            # 5. Failure Log
            self.logger.log_step(
                target_table=target_table_name,
                status="FAILED",
                start_ts=start_ts,
                error_message=str(e)
            )
            raise e

    def run_all(self):
        """Iterates through all YAML files in the config directory."""
        for filename in os.listdir(self.config_dir):
            if filename.endswith(".yml"):
                table_name = filename.replace(".yml", "")
                self.run_table(table_name)

# Entry point for Databricks Job
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark_session = SparkSession.builder.getOrCreate()
    
    # Handle parameter for targeted refresh, otherwise run all
    # table_param = dbutils.widgets.get("table") if running in notebook
    orchestrator = GoldOrchestrator(spark_session)
    orchestrator.run_all()