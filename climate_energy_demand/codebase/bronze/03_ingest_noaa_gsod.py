# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: NOAA GSOD — Weather Station Observations (Global Validation)
# MAGIC
# MAGIC This notebook ingests physical weather station data from the NOAA Global Summary of the Day (GSOD) archive. It serves as a "Ground Truth" source to validate the gridded reanalysis data from Open-Meteo.
# MAGIC
# MAGIC ### Architectural Features
# MAGIC * **Data-Driven Station Discovery:** The ingestion is driven by the `reference_locations.csv` master file. For every country/coordinate, the notebook dynamically identifies the closest physical sensor from the NOAA ISD history catalog.
# MAGIC * **Geospatial Precision:** Implements the **Haversine formula** to calculate great-circle distances. This ensures accurate station selection by accounting for Earth's curvature, which is critical for high-latitude regions (e.g., the Nordics) where standard Euclidean distance is distorted.
# MAGIC * **Resilient Ingestion (Checkpointing):** Adopts an "Append-on-Success" pattern per country. The pipeline checks for existing data in the target Delta table and resumes from the last un-fetched country. This prevents data loss and redundant API calls in the event of network or server interruptions.
# MAGIC * **Data Integrity:** Adheres to the Bronze "Raw" standard. Data is ingested in original Imperial units (Fahrenheit and Inches) with all cleaning, unit standardization, and quality flagging deferred to the Trusted (Silver) layer.
# MAGIC
# MAGIC ### Source & Constraints
# MAGIC * **Source:** [NCEI NOAA Global Summary of the Day](https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/)
# MAGIC * **Timeframe:** 2023–2025.
# MAGIC * **Cutoff:** NOAA ceased updates for the GSOD format on **August 29, 2025**. This notebook performs a one-time historical backfill of the final available data.
# MAGIC
# MAGIC ### Technical Notes
# MAGIC * **Station ID Formatting:** Implements strict 5-digit WBAN padding and USAF code normalization to ensure URL compatibility with the NOAA file server.
# MAGIC * **Fault Tolerance:** Includes error-handling to bypass specific station-year gaps without failing the global ingestion batch.

# COMMAND ----------

import pandas as pd
import math
import requests
from io import StringIO
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.noaa_gsod"

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
GSOD_ACCESS_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{station_id}.csv"

# Timeframe for validation
START_YEAR = 2023
END_YEAR = 2025
YEARS = list(range(START_YEAR, END_YEAR + 1))

# COMMAND ----------

# --- STEP 1: SPATIAL MATH (HAVERSINE) ---
def haversine_km(lat1, lon1, lat2, lon2):
    if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in [lat1, lon1, lat2, lon2]):
        return float('inf')
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.asin(math.sqrt(a))

# COMMAND ----------

# --- STEP 2: CHECKPOINTING (IDENTIFY COMPLETED COUNTRIES) ---
finished_countries = []
if spark.catalog.tableExists(TARGET_TABLE):
    finished_countries = [row.country for row in spark.table(TARGET_TABLE).select("country").distinct().collect()]
    print(f"Skipping {len(finished_countries)} countries already in the table.")

# COMMAND ----------

# --- STEP 3: LOAD & FILTER STATION HISTORY ---
print("Fetching NOAA station history...")
response = requests.get(ISD_HISTORY_URL, timeout=60)
response.raise_for_status()

history = pd.read_csv(StringIO(response.text), dtype={"USAF": str, "WBAN": str})
history.columns = [c.strip() for c in history.columns]
history["BEGIN"] = pd.to_numeric(history["BEGIN"], errors="coerce")
history["END"] = pd.to_numeric(history["END"], errors="coerce")

# Filter for stations that were active recently (at least until 2024)
# and have valid coordinates.
covered_stations = history[
    (history["END"] >= 20240101) & 
    history["LAT"].notna() & 
    history["LON"].notna()
].copy()

# COMMAND ----------

# --- STEP 4: LOAD LOCATIONS ---
locations_df = spark.read.csv(REF_PATH, header=True, inferSchema=True)
# Only fetch countries not already finished
to_fetch = locations_df.filter(~col("name").isin(finished_countries)).filter("latitude IS NOT NULL").collect()

print(f"Remaining countries to fetch: {len(to_fetch)}")

# COMMAND ----------

# --- STEP 5: INGESTION LOOP ---
ingestion_ts = datetime.now(timezone.utc).isoformat()

print(f"Starting ingestion for {len(to_fetch)} locations...")

for row in to_fetch:
    country = row.name
    lat, lon = row.latitude, row.longitude
    
    # 1. Calculate distances from this country to all available stations
    covered_stations["dist"] = covered_stations.apply(
        lambda r: haversine_km(lat, lon, r['LAT'], r['LON']), axis=1
    )
    
    # 2. SAFETY CHECK: If no stations are found, skip this country
    if covered_stations.empty:
        print(f"Skipping {country}: No active stations found in this region.")
        continue
        
    # 3. Get the closest station
    nearest = covered_stations.sort_values("dist").iloc[0]
    
    # 4. Format Station ID: USAF + WBAN (ensuring 5-digit padding for WBAN)
    usaf = str(nearest['USAF'])
    wban = str(nearest['WBAN']).split('.')[0].zfill(5) # Fixes the ID formatting
    station_id = f"{usaf}{wban}"
    
    print(f"Fetching {country}: Nearest station {nearest['STATION NAME']} ({station_id}), {round(nearest['dist'], 1)}km away")
    
    country_rows = []
    for year in YEARS:
        url = GSOD_ACCESS_URL.format(year=year, station_id=station_id)
        try:
            res = requests.get(url, timeout=30)
            if res.status_code == 200:
                data = pd.read_csv(StringIO(res.text))
                for _, day in data.iterrows():
                    country_rows.append(Row(
                        country=country,
                        station_id=station_id,
                        station_name=nearest['STATION NAME'],
                        date=str(day['DATE']), 
                        temp=float(day['TEMP']),
                        max=float(day['MAX']),
                        min=float(day['MIN']),
                        prcp=float(day['PRCP']),
                        ingested_at=ingestion_ts
                    ))
        except Exception:
            continue # If one year is missing, try the next
    
    # 5. CHECKPOINT: Save this country's data immediately to the Delta table
    if country_rows:
        spark.createDataFrame(country_rows).write.format("delta").mode("append").saveAsTable(TARGET_TABLE)