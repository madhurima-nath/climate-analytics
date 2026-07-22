# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: NOAA GSOD Station Ingestion
# MAGIC One-time historical pull of station-level daily temperature data, used to
# MAGIC cross-validate the Open-Meteo ERA5 reanalysis data. GSOD was retired by
# MAGIC NCEI on 2025-08-29 — no new data will ever be published, so this is a
# MAGIC permanent one-time backfill (no incremental notebook needed).
# MAGIC
# MAGIC For each of the 5 Phase 1 reference coordinates, the nearest GSOD station
# MAGIC with full 2010-2025 coverage is looked up automatically from NOAA's
# MAGIC official station list (`isd-history.csv`) — station IDs are not
# MAGIC hardcoded, since they can't be reliably sourced from memory.

# COMMAND ----------

import math
import time
import requests
import pandas as pd
from io import StringIO

# COMMAND ----------

# Same reference coordinates used for the Open-Meteo ingestion, so both
# sources describe (approximately) the same physical locations.
COUNTRIES = {
    "France": (48.8566, 2.3522),
    "Germany": (52.5200, 13.4050),
    "Spain": (40.4168, -3.7038),
    "Italy": (41.9028, 12.4964),
    "United Kingdom": (51.5074, -0.1278),
}

START_YEAR = 2010
END_YEAR = 2025  # GSOD retired 2025-08-29; 2025 will be a partial year

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
GSOD_ACCESS_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{station_id}.csv"

# COMMAND ----------

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

# COMMAND ----------

# Download NOAA's official station list and find the nearest well-covered
# station to each reference coordinate.
response = requests.get(ISD_HISTORY_URL, timeout=60)
response.raise_for_status()

history = pd.read_csv(StringIO(response.text), dtype={"USAF": str, "WBAN": str})
history.columns = [c.strip() for c in history.columns]

history["BEGIN"] = pd.to_numeric(history["BEGIN"], errors="coerce")
history["END"] = pd.to_numeric(history["END"], errors="coerce")

covered = history[
    (history["BEGIN"] <= START_YEAR * 10000 + 101)
    & (history["END"] >= END_YEAR * 10000 + 101)
    & history["LAT"].notna()
    & history["LON"].notna()
].copy()

selected_stations = {}

for country, (lat, lon) in COUNTRIES.items():
    covered["distance_km"] = covered.apply(
        lambda row: haversine_km(lat, lon, row["LAT"], row["LON"]), axis=1
    )
    nearest = covered.sort_values("distance_km").iloc[0]
    station_id = f"{nearest['USAF']}{nearest['WBAN']}"
    selected_stations[country] = {
        "station_id": station_id,
        "name": nearest["STATION NAME"],
        "distance_km": round(nearest["distance_km"], 1),
    }
    print(
        f"{country}: {nearest['STATION NAME']} ({station_id}), "
        f"{round(nearest['distance_km'], 1)} km from reference point"
    )

# COMMAND ----------

def fetch_station_year(station_id: str, year: int) -> pd.DataFrame:
    """Fetch one station-year GSOD CSV. Returns an empty DataFrame if missing
    (some stations don't report every year)."""
    url = GSOD_ACCESS_URL.format(year=year, station_id=station_id)
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        return pd.DataFrame()
    return pd.read_csv(StringIO(response.text))

# COMMAND ----------

all_frames = []

for country, info in selected_stations.items():
    station_id = info["station_id"]
    for year in range(START_YEAR, END_YEAR + 1):
        yearly = fetch_station_year(station_id, year)
        if yearly.empty:
            print(f"{country} ({station_id}), {year}: no data, skipping")
            continue
        yearly["country"] = country
        yearly["station_id"] = station_id
        yearly["station_name"] = info["name"]
        all_frames.append(yearly)
        time.sleep(0.3)  # light pacing across ~80 file requests

pdf = pd.concat(all_frames, ignore_index=True)
print(f"Total rows fetched: {len(pdf)}")

# COMMAND ----------

# Keep the core fields. Values stay in GSOD's native units (Fahrenheit) —
# converting to match Open-Meteo's Celsius belongs in the Silver layer,
# not Bronze, which stores raw source data as published.
KEEP_COLUMNS = ["country", "station_id", "station_name", "DATE", "TEMP", "MAX", "MIN", "PRCP"]
pdf = pdf[[c for c in KEEP_COLUMNS if c in pdf.columns]]

df = spark.createDataFrame(pdf)
display(df.limit(10))

# COMMAND ----------

target_table = "climate_energy_demand.bronze.noaa_gsod"

df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"Written to table: {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, station_name, COUNT(*) AS days, MIN(DATE) AS first_date, MAX(DATE) AS last_date
# MAGIC FROM climate_energy_demand.bronze.noaa_gsod
# MAGIC GROUP BY country, station_name