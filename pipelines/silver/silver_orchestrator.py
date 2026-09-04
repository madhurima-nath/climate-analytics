# Databricks notebook source
# DBTITLE 1,Silver Orchestrator
import os
import sys
import yaml
import time
import importlib

# 1. Path Setup (Safe, no Spark calls)
# -------------------------------------------------------------------------
base_path = os.getcwd()
project_root = os.path.abspath(os.path.join(base_path, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

CONFIG_DIR = os.path.join(base_path, 'configs')

print(f"DEBUG: Working in {base_path}")

# 2. Reduced Infrastructure Sleep
# -------------------------------------------------------------------------
# Short sleep to allow Serverless infrastructure to stabilize.
# Reduced from 30s to 10s to avoid timeout issues.
print("Waiting 10s for Serverless infrastructure to stabilize...")
time.sleep(10)

# 3. Safe Spark Handshake
# -------------------------------------------------------------------------
print("Connecting to Spark Engine...")
try:
    # In a notebook, 'spark' is already in the global namespace.
    # We touch a metadata property (.version) first because it's safer.
    print(f"Spark Version: {spark.version}")
    # Run a simple SQL to confirm the active channel is open.
    spark.sql("SELECT 1").collect()
    print("Spark connection established.")
except Exception as e:
    print(f"Initial connection failed: {str(e)}. Retrying in 20s...")
    time.sleep(20)
    spark.sql("SELECT 1").collect()

# 4. Imports (Only after Spark is stable)
# -------------------------------------------------------------------------
import pyspark.sql.functions as F
try:
    from src.common.audit_utils import get_last_watermark, update_audit_log
    print("Project libraries loaded.")
except ImportError as e:
    print(f"Import Error: {e}")
    # List files to help you debug if 'src' is missing
    print(f"Project Root Contents: {os.listdir(project_root)}")
    raise e

# 5. Orchestration Function
# -------------------------------------------------------------------------
def run_silver_orchestration():
    if not os.path.exists(CONFIG_DIR):
        print(f"ERROR: Config folder missing at {CONFIG_DIR}")
        return

    config_files = [f for f in os.listdir(CONFIG_DIR) if f.endswith('.yml')]
    total_configs = len(config_files)
    print(f"\n{'='*70}")
    print(f"Starting Silver Orchestration: {total_configs} tables to process")
    print(f"{'='*70}\n")
    
    completed = 0
    skipped = 0
    failed = 0

    for idx, config_file in enumerate(config_files, 1):
        config_path = os.path.join(CONFIG_DIR, config_file)
        print(f"\n[{idx}/{total_configs}] Processing: {config_file}")
        print(f"Time: {time.strftime('%H:%M:%S')}")
        
        try:
            with open(config_path, 'r') as f:
                cfg = yaml.safe_load(f)
            
            target_table = cfg['target_table']
            last_ts = get_last_watermark(target_table)
            
            # Extract
            sources = {}
            for key, table_path in cfg['sources'].items():
                df = spark.table(table_path)
                if cfg.get('watermark_column'):
                    sources[key] = df.filter(F.col(cfg['watermark_column']) > last_ts)
                else:
                    sources[key] = df

            if sources and all(df.isEmpty() for df in sources.values()):
                print(f"✅ No new data. Skipping {target_table}")
                skipped += 1
                continue

            # Transform
            module = importlib.import_module(f"src.transforms.{cfg['module']}")
            transform_func = getattr(module, cfg['function'])
            silver_df = transform_func(sources, cfg.get('params', {}))

            # Load
            row_count = silver_df.count()
            if not spark.catalog.tableExists(target_table):
                silver_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
                print(f"✅ Created {target_table} ({row_count:,} rows)")
            else:
                silver_df.createOrReplaceTempView("v_updates")
                join_cond = " AND ".join([f"t.{k} = s.{k}" for k in cfg['merge_keys']])
                spark.sql(f"MERGE INTO {target_table} t USING v_updates s ON {join_cond} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
                print(f"✅ Merged {target_table} ({row_count:,} rows)")
            
            completed += 1

            # Audit
            if cfg.get('watermark_column'):
                primary_key = list(cfg['sources'].keys())[0]
                new_wm = sources[primary_key].select(F.max(cfg['watermark_column'])).collect()[0][0]
                update_audit_log(target_table, new_wm, row_count)

            spark.catalog.clearCache()

        except Exception as e:
            print(f"❌ Failed {config_file}: {str(e)}")
            failed += 1
            continue
    
    print(f"\n{'='*70}")
    print(f"Orchestration Complete!")
    print(f"  ✅ Completed: {completed}")
    print(f"  ⏭️  Skipped: {skipped}")
    print(f"  ❌ Failed: {failed}")
    print(f"{'='*70}")

# 6. Execution
# -------------------------------------------------------------------------
run_silver_orchestration()