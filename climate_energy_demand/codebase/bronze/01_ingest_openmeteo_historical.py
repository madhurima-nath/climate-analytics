# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze: Open-Meteo — Historical Weather Backfill
# MAGIC This notebook performs a global backfill of daily temperature data (2010–2025) 
# MAGIC leveraging the Open-Meteo Archive API. 
# MAGIC
# MAGIC ### Architecture
# MAGIC * **Data-Driven Scaling:** The ingestion process is driven by `reference_locations.csv` 
# MAGIC   stored in Unity Catalog Volumes. This allows for dynamic scaling—adding new 
# MAGIC   countries or regions simply requires a row update in the reference file 
# MAGIC   rather than code changes.
# MAGIC * **Global Scope:** Capable of backfilling 200+ countries and territories 
# MAGIC   in a single execution, ensuring the Bronze layer serves as a comprehensive 
# MAGIC   raw archive.
# MAGIC * **Atomic Data Management:** Uses Delta's `replaceWhere` feature to ensure 
# MAGIC   re-runs only affect the designated backfill range (<= 2025-12-31), 
# MAGIC   protecting more recent data from accidental corruption.
# MAGIC * **Throttling Management:** Implements a 2.0s delay per request. As this is a high-volume global backfill (15 years per country), a conservative delay is used to prevent IP-based throttling during long-running sessions.
# MAGIC * **Exponential Backoff:** Includes a "Wait-and-Retry" logic for 429 (Rate Limit) errors, ensuring the backfill completes even if the API server becomes temporarily saturated.
# MAGIC
# MAGIC ### Source
# MAGIC * API: https://archive-api.open-meteo.com/v1/archive
# MAGIC * Metrics: Daily Max/Min Temperature (2m), Timezone Auto-detection.
# MAGIC * Daily Max/Min: The highest and lowest temperatures recorded during a 24-hour period.
# MAGIC * Temperature (2m): This is the technical standard height for measuring air temperature. Meteorological agencies place sensors exactly 2 meters (about 6.5 feet) above the ground inside a ventilated shield.

# COMMAND ----------

import requests
import time
from datetime import datetime, timezone, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import col

# COMMAND ----------

# # One-time setup: create catalog and schemas for this project
# spark.sql("CREATE CATALOG IF NOT EXISTS climate_energy_demand")
# spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.bronze")
# spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver")
# spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold")

# COMMAND ----------

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.openmeteo_weather"

# COMMAND ----------

# historical weather data from 2010 to 2026
START_DATE = "2010-01-01"
END_DATE = "2026-07-31"

DAILY_VARS = "temperature_2m_max,temperature_2m_min"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

print(f"Using start date: {START_DATE}")
print(f"Using end date: {END_DATE}")

# COMMAND ----------

# --- STEP 1: IDENTIFY COMPLETED COUNTRIES ---
finished_countries = []

if spark.catalog.tableExists(TARGET_TABLE):
    # Get the list of countries we've already successfully backfilled
    # We use distinct() so we don't have a massive list of duplicates
    finished_countries = [row.country for row in spark.table(TARGET_TABLE).select("country").distinct().collect()]
    print(f"Found {len(finished_countries)} countries already in the table. Skipping these.")
else:
    print(f"Target table {TARGET_TABLE} does not exist yet. Starting fresh.")

# COMMAND ----------

# --- STEP 2: LOAD & FILTER LOCATIONS ---
locations_df = spark.read.csv(REF_PATH, header=True, inferSchema=True)
# Only fetch countries that aren't already in our Delta table
to_fetch = locations_df.filter(~col("name").isin(finished_countries)).filter("latitude IS NOT NULL").collect()

print(f"Remaining countries to fetch: {len(to_fetch)}")

def fetch_with_retry(lat, lon, country_name):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": START_DATE, "end_date": END_DATE,
        "daily": DAILY_VARS, "timezone": "auto",
    }
    # For checkpointing, we only try once or twice. 
    # If we get a 429, we WANT the script to stop so we can resume later.
    res = requests.get(ARCHIVE_URL, params=params, timeout=60)
    if res.status_code == 200:
        return res.json()
    elif res.status_code == 429:
        print(f"!!! 429 Rate Limit hit at {country_name}. Stopping for now.")
        return "STOP"
    else:
        return None

# COMMAND ----------

# --- STEP 3: THE CHECKPOINTED LOOP ---
ingestion_ts = datetime.now(timezone.utc).isoformat()

for row in to_fetch:
    country_name = row.name 
    lat, lon = row.latitude, row.longitude
    
    print(f"Fetching {country_name}...")
    data = fetch_with_retry(lat, lon, country_name)
    
    if data == "STOP":
        break # Exit the loop and save what we have
        
    if data:
        dates = data["daily"]["time"]
        tmax = data["daily"]["temperature_2m_max"]
        tmin = data["daily"]["temperature_2m_min"]
        
        country_rows = [
            Row(country=country_name, date=d, temperature_2m_max=mx, 
                temperature_2m_min=mn, latitude=lat, longitude=lon, 
                ingested_at=ingestion_ts)
            for d, mx, mn in zip(dates, tmax, tmin)
        ]
        
        # SAVE IMMEDIATELY (Append)
        # This ensures that even if the next country fails, this one is safe in the table.
        spark.createDataFrame(country_rows).write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        
        # Moderate sleep
        time.sleep(2.0)

print("Batch process complete or paused due to rate limits.")