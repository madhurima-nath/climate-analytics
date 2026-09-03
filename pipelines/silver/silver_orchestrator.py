import os
import sys
import yaml
import importlib
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. Pathing: Ensure the worker can import from the 'src' library
# -----------------------------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.common.audit_utils import get_last_watermark, update_audit_log

spark = SparkSession.builder.getOrCreate()

# Constants
CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'configs'))

def run_silver_orchestration():
    """
    The main execution loop. 
    Iterates through all YAML files in the configs directory and promotes 
    data from Bronze to Silver incrementally.
    """
    
    # Get all YAML files from the config directory
    config_files = [f for f in os.listdir(CONFIG_DIR) if f.endswith('.yml')]
    
    print(f"Found {len(config_files)} configurations. Beginning ingestion...")

    for config_file in config_files:
        config_path = os.path.join(CONFIG_DIR, config_file)
        
        with open(config_path, 'r') as f:
            cfg = yaml.safe_load(f)
        
        target_table = cfg['target_table']
        print(f"\n--- Processing Table: {target_table} ---")

        try:
            # 2. State Management (Manual Watermarking)
            # -----------------------------------------------------------------
            # Find the last processed timestamp for this specific table.
            last_ts = get_last_watermark(target_table)
            
            # 3. Data Extraction
            # -----------------------------------------------------------------
            # Load all sources defined in the YAML into a dictionary.
            # We filter the primary source (the first one) by the watermark.
            sources = {}
            for key, table_path in cfg['sources'].items():
                if cfg.get('watermark_column'):
                    # Incremental load for the primary data source
                    sources[key] = spark.table(table_path).filter(F.col(cfg['watermark_column']) > last_ts)
                else:
                    # Full load for static dimensions (like stations or peatlands)
                    sources[key] = spark.table(table_path)

            # Check if there is new data to process
            # (If sources is empty, like in dim_date, we proceed to generation)
            if sources and all(df.isEmpty() for df in sources.values()):
                print(f"No new data for {target_table}. Skipping.")
                continue

            # 4. Dynamic Logic Execution
            # -----------------------------------------------------------------
            # Load the domain module (e.g., src.transforms.weather)
            module = importlib.import_module(f"src.transforms.{cfg['module']}")
            # Get the specific function (e.g., process_weather_historical)
            transform_func = getattr(module, cfg['function'])
            
            # Execute the transformation
            silver_df = transform_func(sources, cfg.get('params', {}))

            # 5. Delta Merge (Upsert) Logic
            # -----------------------------------------------------------------
            # We use the 'merge_keys' from the YAML to prevent duplicates.
            if not spark.catalog.tableExists(target_table):
                # First-time load: Create table and apply Genie description
                silver_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
                spark.sql(f"COMMENT ON TABLE {target_table} IS '{cfg['description']}'")
                print(f"Table {target_table} created.")
            else:
                # Incremental load: Perform a Delta MERGE
                silver_df.createOrReplaceTempView("v_updates")
                
                # Dynamically build the join condition from the YAML merge_keys
                join_condition = " AND ".join([f"t.{k} = s.{k}" for k in cfg['merge_keys']])
                
                merge_sql = f"""
                    MERGE INTO {target_table} t
                    USING v_updates s
                    ON {join_condition}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """
                spark.sql(merge_sql)
                print(f"Delta Merge complete for {target_table}.")

            # 6. Finalise Audit
            # -----------------------------------------------------------------
            if cfg.get('watermark_column'):
                # Extract the latest ingested_at timestamp from the primary source
                primary_source_key = list(cfg['sources'].keys())[0]
                new_watermark = sources[primary_source_key].select(F.max(cfg['watermark_column'])).collect()[0][0]
                update_audit_log(target_table, new_watermark, silver_df.count())
                print(f"Audit log updated with watermark: {new_watermark}")

        except Exception as e:
            print(f"ERROR processing {target_table}: {str(e)}")
            # On Free Edition, we log the error but continue to the next table 
            # to maximise the 2-hour compute window.
            continue

if __name__ == "__main__":
    run_silver_orchestration()