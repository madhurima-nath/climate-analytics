# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Open-Meteo — Incremental Weather Update
# MAGIC This notebook provides a robust daily update mechanism for the historical 
# MAGIC weather series, ensuring the dataset remains current with zero manual intervention.
# MAGIC
# MAGIC ### Technical Implementation
# MAGIC * **State-Aware Logic:** Automatically determines the update range by 
# MAGIC   querying the existing Delta table for the latest available date per country.
# MAGIC * **Refined Observations:** Implements a 7-day look-back window. This ensures 
# MAGIC   the pipeline captures corrections often made by meteorological agencies 
# MAGIC   a few days after initial data publication.
# MAGIC * **Idempotency:** Uses the Delta `MERGE` operation to handle overlapping 
# MAGIC   data windows. This prevents duplicates and ensures the pipeline can be 
# MAGIC   re-run safely without corrupting the target table.
# MAGIC * **Resilient Loop:** Uses an error-handling pattern to ensure that an 
# MAGIC   API failure for a single coordinate does not block the update 
# MAGIC   process for the rest of the global dataset.
# MAGIC * **Optimized Performance:** Uses a 1.5s delay per request. Since incremental updates are "lighter" (7–30 days) than backfills, the delay is reduced to optimize daily runtime while remaining well within the API's fair-use limits.
# MAGIC * **Shared Resilience:** Shares the same robust 429 error handling as the historical pipeline, ensuring the daily automated update is resilient to temporary API instability.
# MAGIC
# MAGIC ### Source
# MAGIC * API: https://api.open-meteo.com/v1/forecast
# MAGIC * Metrics: Daily Max/Min Temperature (2m).
# MAGIC * Daily Max/Min: The highest and lowest temperatures recorded during a 24-hour period.
# MAGIC * Temperature (2m): This is the technical standard height for measuring air temperature. Meteorological agencies place sensors exactly 2 meters (about 6.5 feet) above the ground inside a ventilated shield.

# COMMAND ----------

import requests
import time
from datetime import datetime, timezone, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import max as spark_max

# COMMAND ----------

# One-time setup: create catalog and schemas for this project
spark.sql("CREATE CATALOG IF NOT EXISTS climate_energy_demand")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold")

# COMMAND ----------

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.openmeteo_weather"

# Forecast API is better for incremental as it has 0-day lag
API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS = "temperature_2m_max,temperature_2m_min"

# COMMAND ----------


# --- STEP 1: STATE-AWARE SETUP ---
# 1. Load the dynamic country list
locations_df = spark.read.csv(REF_PATH, header=True, inferSchema=True)
locations = locations_df.filter("latitude IS NOT NULL").collect()

# 2. Get latest dates from existing table to determine "where we left off"
if spark.catalog.tableExists(TARGET_TABLE):
    last_dates_df = spark.table(TARGET_TABLE).groupBy("country").agg(spark_max("date").alias("max_date"))
    last_dates = {row.country: row.max_date for row in last_dates_df.collect()}
else:
    print("Warning: Target table not found. Defaulting to 30-day lookback.")
    last_dates = {}

def fetch_with_retry(lat, lon, start_date, end_date, country_name, max_retries=3):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": DAILY_VARS, "timezone": "auto"
    }
    for attempt in range(max_retries):
        try:
            res = requests.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                wait_time = 60 * (attempt + 1)
                print(f"!!! Rate limited (429) for {country_name}. Waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Error {res.status_code} for {country_name}")
                return None
        except Exception as e:
            print(f"Request failed for {country_name}: {e}")
            time.sleep(5)
    return None

all_rows = []
ingestion_ts = datetime.now(timezone.utc).isoformat()
today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# COMMAND ----------

# --- STEP 2: INCREMENTAL LOOP ---
print(f"Starting incremental update for {len(locations)} locations...")

for row in locations:
    country = row.name
    lat = row.latitude
    lon = row.longitude
    
    # 7-day look-back to catch data corrections from weather agencies
    last_date_str = last_dates.get(country)
    if last_date_str:
        start_dt = datetime.strptime(last_date_str, "%Y-%m-%d") - timedelta(days=7)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=30)
    
    start_date = start_dt.strftime("%Y-%m-%d")
    
    # If we are already up to date, skip
    if start_date >= today_str:
        continue

    print(f"Updating {country} from {start_date} to {today_str}...")
    
    data = fetch_with_retry(lat, lon, start_date, today_str, country)
    
    if data:
        dates = data["daily"]["time"]
        tmax = data["daily"]["temperature_2m_max"]
        tmin = data["daily"]["temperature_2m_min"]

        for d, mx, mn in zip(dates, tmax, tmin):
            all_rows.append(Row(
                country=country, date=d, 
                temperature_2m_max=mx, temperature_2m_min=mn, 
                latitude=lat, longitude=lon, 
                ingested_at=ingestion_ts
            ))
        # Be gentle with the API
        time.sleep(1.5)

# COMMAND ----------

# --- STEP 3: UPSERT (MERGE) INTO DELTA ---
if all_rows:
    new_data_df = spark.createDataFrame(all_rows)
    new_data_df.createOrReplaceTempView("updates")
    
    # The MERGE ensures no duplicates even with the 7-day look-back
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS target
        USING updates AS source
        ON target.country = source.country AND target.date = source.date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Update successful. Added/Updated {len(all_rows)} rows.")
else:
    print("No new data to ingest.")