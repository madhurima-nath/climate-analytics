# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Open-Meteo CMIP6 Climate Projections Ingestion
# MAGIC This notebook ingests future climate model projections for all global reference 
# MAGIC locations, providing a multi-model ensemble for long-term risk assessment.
# MAGIC
# MAGIC ### Technical Architecture
# MAGIC * **Probabilistic Ensemble:** Ingests data from all 7 available CMIP6 HighResMIP models 
# MAGIC   (e.g., EC-Earth3P-HR, MRI-AGCM3-2-S). The variance across these models provides 
# MAGIC   the necessary "uncertainty signal" required for climate risk modeling in the 
# MAGIC   absence of specific SSP scenario selectors.
# MAGIC * **Strategic Sampling:** Pulls complete calendar years (Jan-Dec) for specific intervals 
# MAGIC   (2020–2050, every 5th year). This allows for accurate annual Heating/Cooling Degree 
# MAGIC   Day (HDD/CDD) calculations while significantly reducing API overhead compared to 
# MAGIC   a continuous 100-year pull.
# MAGIC * **Fault-Tolerant Checkpointing:** Tracks progress at the **(Country, Year)** grain. 
# MAGIC   The pipeline automatically resumes from the last successful ingestion point, 
# MAGIC   making it resilient against API throttling and network interruptions during 
# MAGIC   large-scale global backfills.
# MAGIC * **Throttling Management:** Implements an exponential backoff (5-minute cooldown) 
# MAGIC   for 429 rate-limit errors and a conservative 3-second delay between requests 
# MAGIC   to accommodate the heavy payload of the 7-model daily data.
# MAGIC
# MAGIC ### Source
# MAGIC * API: https://climate-api.open-meteo.com/v1/climate
# MAGIC * Parameters: Daily Mean/Max/Min Temperature (2m).

# COMMAND ----------

import requests
import time
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.functions import col, expr

# COMMAND ----------

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.openmeteo_climate_cmip6_projections"

SAMPLE_YEARS = [2020, 2025, 2030, 2035, 2040, 2045, 2050]
MODELS = [
    "CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S",
    "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S",
]
DAILY_VARS = "temperature_2m_mean,temperature_2m_max,temperature_2m_min"
CLIMATE_URL = "https://climate-api.open-meteo.com/v1/climate"

# COMMAND ----------

# --- STEP 1: CHECKPOINTING (IDENTIFY COMPLETED WORK) ---
finished_pairs = set()
if spark.catalog.tableExists(TARGET_TABLE):
    # We identify which country-year combinations are already in the table
    # Using YEAR(date) to extract the sample year
    pairs_df = spark.table(TARGET_TABLE).select("country", expr("YEAR(date)").alias("year")).distinct()
    finished_pairs = {(row.country, row.year) for row in pairs_df.collect()}
    print(f"Skipping {len(finished_pairs)} country-year combinations already in the table.")

# COMMAND ----------

# --- STEP 2: LOAD DYNAMIC LOCATIONS ---
locations_df = spark.read.csv(REF_PATH, header=True, inferSchema=True)
locations = locations_df.filter("latitude IS NOT NULL").collect()

# COMMAND ----------

# --- STEP 3: API LOGIC WITH RETRY ---
def fetch_with_retry(lat, lon, year, country_name, max_retries=3):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": f"{year}-01-01", "end_date": f"{year}-12-31",
        "models": ",".join(MODELS),
        "daily": DAILY_VARS,
    }
    for attempt in range(max_retries):
        try:
            res = requests.get(CLIMATE_URL, params=params, timeout=60)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                wait_time = 300 * (attempt + 1)
                print(f"!!! Rate limit (429) at {country_name} for {year}. Waiting {wait_time/60} mins...")
                time.sleep(wait_time)
            else:
                print(f"Error {res.status_code} for {country_name}")
                return None
        except Exception as e:
            print(f"Request failed: {e}")
            time.sleep(10)
    return None

# COMMAND ----------

# --- STEP 4: GLOBAL INGESTION LOOP ---
ingestion_ts = datetime.now(timezone.utc).isoformat()

for year in SAMPLE_YEARS:
    for row in locations:
        country = row.name
        lat, lon = row.latitude, row.longitude
        
        # Check if this specific country-year is already done
        if (country, year) in finished_pairs:
            continue
            
        print(f"Fetching Projections for {country} / {year}...")
        data = fetch_with_retry(lat, lon, year, country)
        
        if data:
            daily = data["daily"]
            dates = daily["time"]
            country_rows = []

            for model in MODELS:
                tmean = daily.get(f"temperature_2m_mean_{model}")
                tmax = daily.get(f"temperature_2m_max_{model}")
                tmin = daily.get(f"temperature_2m_min_{model}")
                
                if tmean is None: continue

                for d, m, x, n in zip(dates, tmean, tmax, tmin):
                    country_rows.append(Row(
                        country=country, date=d, model=model,
                        temperature_2m_mean=m, temperature_2m_max=x, 
                        temperature_2m_min=n, latitude=lat, longitude=lon,
                        ingested_at=ingestion_ts
                    ))
            
            # Save country-year immediately (Checkpoint)
            if country_rows:
                spark.createDataFrame(country_rows).write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
                
            # Heavier payload (7 models) requires a more conservative sleep
            time.sleep(3.0) 
        else:
            print(f"--- FAILED: {country} / {year} ---")

print("Climate Projections ingestion batch complete.")