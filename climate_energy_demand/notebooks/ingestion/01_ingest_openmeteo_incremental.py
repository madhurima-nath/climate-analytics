# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Open-Meteo Historical Weather Ingestion
# MAGIC Pulls daily ERA5 temperature data (2010-2025) for the 5 Phase 1 countries
# MAGIC and writes raw results to a bronze Delta table.
# MAGIC
# MAGIC Safe to rerun: only overwrites rows within its own date range (see
# MAGIC `replaceWhere` below), so it will not erase newer data added by
# MAGIC `02_ingest_openmeteo_incremental.py`.
# MAGIC
# MAGIC API: https://open-meteo.com/en/docs/historical-weather-api

# COMMAND ----------

import requests
import time
from datetime import datetime, UTC
from pyspark.sql import Row

# COMMAND ----------

# One-time setup: create catalog and schemas for this project
spark.sql("CREATE CATALOG IF NOT EXISTS climate_energy_demand")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold")

# Drop the old table from the default location (recreating clean in the new catalog)
spark.sql("DROP TABLE IF EXISTS bronze_openmeteo_historical")

# COMMAND ----------

# Representative coordinates per country (capital cities)
COUNTRIES = {
    "France": (48.8566, 2.3522),
    "Germany": (52.5200, 13.4050),
    "Spain": (40.4168, -3.7038),
    "Italy": (41.9028, 12.4964),
    "United Kingdom": (51.5074, -0.1278),
}

START_DATE = "2010-01-01"
END_DATE = "2025-12-31"

DAILY_VARS = "temperature_2m_max,temperature_2m_min"

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# COMMAND ----------

def fetch_country_history(country: str, lat: float, lon: float) -> dict:
    """Call the Open-Meteo archive API for one country's coordinates."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": DAILY_VARS,
        "timezone": "auto",
    }
    response = requests.get(ARCHIVE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

all_rows = []
ingestion_ts = datetime.now(UTC).isoformat()

for country, (lat, lon) in COUNTRIES.items():
    print(f"Fetching {country}...")
    data = fetch_country_history(country, lat, lon)

    dates = data["daily"]["time"]
    temp_max = data["daily"]["temperature_2m_max"]
    temp_min = data["daily"]["temperature_2m_min"]

    for d, tmax, tmin in zip(dates, temp_max, temp_min):
        all_rows.append(
            Row(
                country=country,
                date=d,
                temperature_2m_max=tmax,
                temperature_2m_min=tmin,
                latitude=lat,
                longitude=lon,
                ingested_at=ingestion_ts,
            )
        )

    # brief pause between requests to stay within Open-Meteo's fair-use limits
    time.sleep(1)

print(f"Total rows fetched: {len(all_rows)}")

# COMMAND ----------

df = spark.createDataFrame(all_rows)
display(df.limit(10))

# COMMAND ----------

target_table = "climate_energy_demand.bronze.openmeteo_historical"

if spark.catalog.tableExists(target_table):
    # Only replace rows within this backfill's own range — leaves any
    # newer rows added by the incremental notebook untouched.
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"date <= '{END_DATE}'")
        .saveAsTable(target_table)
    )
else:
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"Written to table: {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, COUNT(*) AS days, MIN(date) AS first_date, MAX(date) AS last_date
# MAGIC FROM climate_energy_demand.bronze.openmeteo_historical
# MAGIC GROUP BY country