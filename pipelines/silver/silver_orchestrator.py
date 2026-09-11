# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Orchestrator
import os
import sys
import yaml
import time
import uuid
import importlib
from datetime import datetime

# 1. Path Setup (Safe, no Spark calls)
# -------------------------------------------------------------------------
base_path = os.getcwd()
project_root = os.path.abspath(os.path.join(base_path, "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)

CONFIG_DIR = os.path.join(base_path, 'configs')

print(f"Working directory: {base_path}")

# 2. Imports
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

# 3. Helper for Run ID
# -------------------------------------------------------------------------
# On Serverless, spark.conf.get("spark.databricks.job.runId") is not accessible.
# Instead, the job should pass the run ID as a task parameter using a dynamic
# value reference: {"job_run_id": "{{job.run_id}}"} in the job's task parameters.
# The widget provides a default empty value for interactive (non-job) runs.
dbutils.widgets.text("job_run_id", "", "Job Run ID")

def get_run_id():
    """Retrieves the Databricks Job Run ID via widget parameter."""
    try:
        job_run_id = dbutils.widgets.get("job_run_id")
        if job_run_id:
            return f"job-{job_run_id}"
    except Exception:
        pass
    return f"manual-{int(time.time())}"

# 3b. Monitoring: Read layer/task_key, capture run_id, log run start
# -------------------------------------------------------------------------
# Use raw job_run_id for consistency with validation notebook (no "job-" prefix)
try:
    _orch_run_id = dbutils.widgets.get("job_run_id")
    if not _orch_run_id:
        _orch_run_id = f"manual-{int(time.time())}"
except Exception:
    _orch_run_id = f"manual-{int(time.time())}"

try:
    layer = dbutils.widgets.get("layer")
except Exception:
    layer = "silver"
try:
    task_key = dbutils.widgets.get("task_key")
except Exception:
    task_key = "run_orchestrator"
try:
    job_name = dbutils.widgets.get("job_name")
except Exception:
    job_name = "standalone"

_orch_start_time = datetime.now()

def _update_pipeline_run(status, error_message=None, duration_seconds=None,
                          total_configs=None, completed=None, skipped=None, failed=None):
    """MERGE final status into pipeline_runs for this run."""
    set_parts = [f"status = '{status}'"]
    if error_message is not None:
        safe_msg = error_message.replace("'", "''")
        set_parts.append(f"error_message = '{safe_msg}'")
    if duration_seconds is not None:
        set_parts.append(f"duration_seconds = {duration_seconds}")
    if total_configs is not None:
        set_parts.append(f"total_configs = {total_configs}")
    if completed is not None:
        set_parts.append(f"configs_completed = {completed}")
    if skipped is not None:
        set_parts.append(f"configs_skipped = {skipped}")
    if failed is not None:
        set_parts.append(f"configs_failed = {failed}")
    set_clause = ", ".join(set_parts)
    spark.sql(f"""
        MERGE INTO climate_energy_demand.monitoring.pipeline_runs AS t
        USING (SELECT '{_orch_run_id}' AS run_id, '{task_key}' AS task_key) AS s
        ON t.run_id = s.run_id AND t.task_key = s.task_key
        WHEN MATCHED THEN UPDATE SET {set_clause}
    """)

# INSERT "running" row into pipeline_runs
spark.sql(f"""
    INSERT INTO climate_energy_demand.monitoring.pipeline_runs
    (run_timestamp, job_name, task_key, task_type, status, run_id, layer)
    VALUES (TIMESTAMP '{_orch_start_time}', '{job_name}', '{task_key}', 'orchestration', 'running', '{_orch_run_id}', '{layer}')
""")
print(f"Monitoring: Logged run start (run_id={_orch_run_id}, layer={layer}, task_key={task_key})")

# 4. Orchestration Function
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
    _detail_completed = []
    _detail_skipped = []
    _detail_failed = []

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
                _detail_skipped.append(f"{target_table} (no new data)")
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
                view_name = f"v_updates_{uuid.uuid4().hex}"
                silver_df.createOrReplaceTempView(view_name)
                join_cond = " AND ".join([f"t.{k} = s.{k}" for k in cfg['merge_keys']])
                spark.sql(f"MERGE INTO {target_table} t USING {view_name} s ON {join_cond} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
                print(f"✅ Merged {target_table} ({row_count:,} rows)")
            
            _detail_completed.append(f"{target_table} ({row_count:,} rows)")
            completed += 1

            # Audit (only if rows were processed)
            if cfg.get('watermark_column') and row_count > 0:
                primary_key = list(cfg['sources'].keys())[0]
                new_wm = sources[primary_key].select(F.max(cfg['watermark_column'])).collect()[0][0]
                update_audit_log(target_table, new_wm, row_count)

            # spark.catalog.clearCache()  # Not supported on serverless compute

        except Exception as e:
            print(f"❌ Failed {config_file}: {str(e)}")
            _detail_failed.append(f"{config_file}: {str(e)}")
            failed += 1
            continue
    
    print(f"\n{'='*70}")
    print(f"Orchestration Complete!")
    print(f"  ✅ Completed: {completed}")
    print(f"  ⏭️  Skipped: {skipped}")
    print(f"  ❌ Failed: {failed}")
    print(f"{'='*70}")

    # Log to Monitoring Table (pipeline_runs)
    try:
        _orch_duration = int((datetime.now() - _orch_start_time).total_seconds())
        _orch_status = "failed" if failed > 0 else "completed"
        
        # Build per-table summary for error_message (visible on dashboard)
        _summary_parts = []
        if _detail_completed:
            _summary_parts.append("Completed: " + ", ".join(_detail_completed))
        if _detail_skipped:
            _summary_parts.append("Skipped: " + ", ".join(_detail_skipped))
        if _detail_failed:
            _summary_parts.append("Failed: " + ", ".join(_detail_failed))
        _orch_summary = " | ".join(_summary_parts) if _summary_parts else "No tables processed"
        
        _update_pipeline_run(_orch_status, error_message=_orch_summary,
                            duration_seconds=_orch_duration,
                            total_configs=total_configs, completed=completed,
                            skipped=skipped, failed=failed)
        print(f"Monitoring: Updated pipeline_runs (status={_orch_status}, run_id={_orch_run_id})")
        
    except Exception as e:
        print(f"Failed to log monitoring: {e}")

# 5. Execution
# -------------------------------------------------------------------------
run_silver_orchestration()