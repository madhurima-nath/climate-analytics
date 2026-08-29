# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Open-Meteo CMIP6 Climate Projections Ingestion
# MAGIC This notebook ingests future climate model projections for all global reference 
# MAGIC locations, providing a multi-model ensemble for long-term risk assessment.
# MAGIC
# MAGIC ***Purpose:*** Ingest 7-model HighResMIP ensemble data for EU energy demand modeling.
# MAGIC **Environment:** Databricks Single-Node (Free Edition)
# MAGIC **Target Table:** `bronze.openmeteo_climate_cmip6_projections`
# MAGIC
# MAGIC ### Operational Workflow
# MAGIC 1.  **Checkpointing:** Scans Delta table for existing `(Country, Year)` pairs to generate a delta-workload.
# MAGIC 2.  **Strategic Sampling:** Pulls 5-year intervals (2020-2050) to optimize HDD/CDD calculations.
# MAGIC 3.  **Sequential Commits:** Saves to disk after **every batch** (50 locations). This is a safety feature to prevent data loss if the Databricks Free Tier 10-minute timeout is reached.
# MAGIC 4.  **Progress Tracking:** Monitors `[Batch / Total]` progress with completion percentages in real-time.
# MAGIC
# MAGIC ### Controls
# MAGIC - **Resiliency:** Safe to stop/start at any time.
# MAGIC - **Throttle:** 2.0s cooldown per request + exponential backoff for 429 errors.
# MAGIC - **Format:** Tidy Data (One row per date/country/model).
# MAGIC
# MAGIC
# MAGIC ### Source
# MAGIC * API: https://climate-api.open-meteo.com/v1/climate
# MAGIC * Parameters: Daily Mean/Max/Min Temperature (2m).

# COMMAND ----------

import requests
import time
import pandas as pd
from datetime import datetime, timezone
from typing import List, Set, Tuple, Optional
from pyspark.sql import functions as F

# COMMAND ----------

# global configuration
CATALOG, SCHEMA = "climate_energy_demand", "bronze"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.openmeteo_climate_cmip6_projections"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"

CLIMATE_URL = "https://climate-api.open-meteo.com/v1/climate"
SAMPLE_YEARS = [2020, 2025, 2030, 2035, 2040, 2045, 2050]
MODELS = ["CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S", "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S"]
DAILY_VARS = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min"]

BATCH_SIZE = 5         # Max locations per request

# COMMAND ----------

## state management - checking to never download the same data twice

def get_ingestion_checkpoint(table_name: str) -> Set[Tuple[str, int]]:
    """Scans Delta table to return processed (Country, Year) combinations."""
    if not spark.catalog.tableExists(table_name):
        print(f"Target table {table_name} not found. Initializing fresh ingestion.")
        return set()
    
    existing_df = spark.table(table_name).select("country", F.year("date").alias("year")).distinct()
    return {(r.country, r.year) for r in existing_df.collect()}

# COMMAND ----------

## get data in chunks
def fetch_api_batch(chunk: List, year: int, max_retries: int = 3) -> Optional[List[dict]]:
    """Handles API request with coordinate batching and basic retry logic."""
    params = {
        "latitude": ",".join([str(c.latitude) for c in chunk]),
        "longitude": ",".join([str(c.longitude) for c in chunk]),
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "models": ",".join(MODELS),
        "daily": ",".join(DAILY_VARS),
        "timezone": "UTC"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(CLIMATE_URL, params=params, timeout=120)
            if response.status_code == 200:
                data = response.json()
                return data if isinstance(data, list) else [data]
            elif response.status_code == 429:
                print(f"Rate limited (429). Attempt {attempt+1}: Cooling down...")
                time.sleep(45 * (attempt + 1))
            else:
                print(f"API Failure ({response.status_code}): {response.text}")
        except Exception as e:
            print(f"Request Exception: {str(e)}")
            time.sleep(10)
    return None

# COMMAND ----------

## transformation of json

def vectorized_transform(api_response: List[dict], metadata: List) -> pd.DataFrame:
    """Vectorized transformation of nested API JSON into Tidy Data format."""
    batch_dfs = []
    for i, entry in enumerate(api_response):
        if "daily" not in entry: continue
        
        # 1. Load into Pandas
        df = pd.DataFrame(entry["daily"])
        df["country"] = metadata[i].name
        df["latitude"], df["longitude"] = entry["latitude"], entry["longitude"]
        
        # 2. Reshape from wide to long (Uncertainty Signal modeling)
        melted = df.melt(id_vars=["time", "country", "latitude", "longitude"], var_name="raw_var")
        
        # Extract Variable and Model from column names
        melted[['variable', 'model']] = melted['raw_var'].str.extract(r'^(temperature_2m_\w+?)_(.*)$')
        
        # 3. Pivot back to clean schema
        pivoted = melted.pivot_table(
            index=["time", "country", "latitude", "longitude", "model"],
            columns="variable", 
            values="value"
        ).reset_index()
        
        batch_dfs.append(pivoted)
        
    return pd.concat(batch_dfs) if batch_dfs else pd.DataFrame()

# COMMAND ----------

# initialize ingestion state
processed_tasks = get_ingestion_checkpoint(TARGET_TABLE)

# load reference data
ref_df = spark.read.csv(REF_PATH, header=True, inferSchema=True).filter("latitude IS NOT NULL")
ref_locations = ref_df.collect()

# generate work queue
work_queue = []
for year in SAMPLE_YEARS:
    todo = [loc for loc in ref_locations if (loc.name, year) not in processed_tasks]
    for i in range(0, len(todo), BATCH_SIZE):
        work_queue.append((year, todo[i : i + BATCH_SIZE]))

# calculate metrics
total_countries = len(ref_locations)
total_years = len(SAMPLE_YEARS)
grand_total_tasks = total_countries * total_years
tasks_completed = len(processed_tasks)
tasks_remaining = grand_total_tasks - tasks_completed
completion_pct = (tasks_completed / grand_total_tasks) * 100 if grand_total_tasks > 0 else 0

# summary 
print("-" * 50)
print(f"Ingestion Pipeline Status | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 50)
print(f"Total Reference Countries:  {total_countries}")
print(f"Total Projection Years:    {total_years} ({', '.join(map(str, SAMPLE_YEARS))})")
print(f"Grand Total Workload:      {grand_total_tasks} Country-Year tasks")
print("-" * 50)
print(f"Lineage Check:             {tasks_completed} tasks already in Delta Lake")
print(f"Current Gap:               {tasks_remaining} tasks missing")
print(f"Work Queue:                {len(work_queue)} batches pending (Batch Size: {BATCH_SIZE})")
print(f"Project Completion:        {completion_pct:.2f}%")
print("-" * 50)

if len(work_queue) == 0:
    print("STATUS: Data is fully synchronized. No execution required.")
else:
    print(f"STATUS: Ready to ingest {len(work_queue)} batches.")

# COMMAND ----------

# Final Execution with high-granularity logging
ingestion_timestamp = datetime.now(timezone.utc)
total_batches = len(work_queue)

for idx, (year, chunk) in enumerate(work_queue):
    start_time = time.time()
    print(f"--- Batch {idx + 1}/{total_batches} | Target: {year} | Count: {len(chunk)} ---")
    
    # 1. Network Fetch
    raw_json = fetch_api_batch(chunk, year)
    
    if raw_json:
        # 2. Vectorized Transform
        processed_pdf = vectorized_transform(raw_json, chunk)
        
        if not processed_pdf.empty:
            # 3. Delta Persistence
            (spark.createDataFrame(processed_pdf)
                  .withColumn("date", F.col("time").cast("date"))
                  .withColumn("ingested_at", F.lit(ingestion_timestamp))
                  .drop("time")
                  .write.format("delta")
                  .mode("append")
                  .saveAsTable(TARGET_TABLE))
            
            elapsed = time.time() - start_time
            print(f"    [SUCCESS] Batch {idx+1} committed in {elapsed:.2f}s.")
        else:
            print(f"    [WARNING] Batch {idx+1} returned no data.")
    else:
        print(f"    [ERROR] Batch {idx+1} failed after retries.")

    # 4. API Politeness
    time.sleep(2.0)

print("Ingestion Job Terminated Successfully.")