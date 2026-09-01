# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Incremental Weather Data Ingestion Pipeline
# MAGIC #### Data Tier: Bronze | Catalog: `climate_energy_demand`
# MAGIC
# MAGIC ## 1. Overview
# MAGIC This notebook implements a modular, state-aware ETL pipeline designed to ingest daily weather variables (Max/Min Temperature) from the **Open-Meteo Archive API**. The pipeline is architected for **incremental loading**, specifically bridging the gap between existing historical data (last finalized on **July 31, 2026**) and the current system date.
# MAGIC
# MAGIC ## 2. Key Architectural Principles
# MAGIC *   **Idempotency (No Duplicates):** The pipeline uses **Delta Lake MERGE** logic. If a job is re-run or date ranges overlap, existing records are updated with the most recent API data rather than creating duplicate entries.
# MAGIC *   **State Management (Watermarking):** Instead of hardcoded dates, the system queries the `TARGET_TABLE` to dynamically identify the "High Water Mark" (latest ingested date) for each country.
# MAGIC *   **Data Integrity & Revisions:** To account for upstream data corrections often made by weather agencies, the pipeline implements a **7-day look-back overlap**. It re-fetches the last week of previously ingested data to ensure "provisional" records are updated to "finalized" values.
# MAGIC *   **Unity Catalog Integration:** The solution leverages **UC Volumes** for reference data management and uses a three-level namespace for target table governance.
# MAGIC
# MAGIC ## 3. Data Flow
# MAGIC 1.  **Extract:** Load dynamic country coordinates from a reference CSV stored in UC Volumes.
# MAGIC 2.  **State Check:** Determine the latest available date in the `openmeteo_weather` table for each location.
# MAGIC 3.  **Fetch:** Request missing historical data from the Archive API (optimized via `requests.Session`).
# MAGIC 4.  **Transform:** Cast API responses into a structured Spark schema, ensuring type consistency for temperatures and coordinates.
# MAGIC 5.  **Load:** Perform an atomic Upsert (Merge) into the Bronze Delta table.
# MAGIC
# MAGIC ## 4. Pipeline Parameters
# MAGIC | Variable | Value | Description |
# MAGIC |----------|-------|-------------|
# MAGIC | **Target Table** | `openmeteo_weather` | The destination Bronze Delta table. |
# MAGIC | **Reference Path** | `reference_locations.csv` | Source of truth for latitudes and longitudes. |
# MAGIC | **API Endpoint** | `v1/archive` | Used to retrieve finalized historical weather observations. |
# MAGIC | **Lookback Buffer** | 7 Days | Overlap period used to synchronize data corrections. |
# MAGIC
# MAGIC ### Source
# MAGIC * API: https://api.open-meteo.com/v1/forecast
# MAGIC * Metrics: Daily Max/Min Temperature (2m).
# MAGIC * Daily Max/Min: The highest and lowest temperatures recorded during a 24-hour period.
# MAGIC * Temperature (2m): This is the technical standard height for measuring air temperature. Meteorological agencies place sensors exactly 2 meters (about 6.5 feet) above the ground inside a ventilated shield.

# COMMAND ----------

import requests
import time
import logging
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import max as spark_max
from delta.tables import DeltaTable

# COMMAND ----------

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WeatherETL")

# COMMAND ----------

CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
REF_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/reference_locations.csv"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.openmeteo_weather"

# Using Archive API to support historical incremental loads (July 2024 to 2026)
API_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = "temperature_2m_max,temperature_2m_min"

# COMMAND ----------

# api client
class WeatherClient:
    """Handles network communication with Open-Meteo."""
    def __init__(self):
        self.session = requests.Session()

    def fetch_with_retry(self, lat, lon, start_date, end_date, country_name):
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": start_date, "end_date": end_date,
            "daily": DAILY_VARS, "timezone": "auto"
        }
        try:
            res = self.session.get(API_URL, params=params, timeout=60)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                logger.warning(f"Rate limit hit for {country_name}. Throttling...")
                time.sleep(30)
            return None
        except Exception as e:
            logger.error(f"Request failed for {country_name}: {e}")
            return None

# COMMAND ----------

# state management - don't call already loaded data
class StateManager:
    """Handles Watermarking to find where the data last stopped."""
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_last_ingested_dates(self) -> Dict[str, date]:
        if not self.spark.catalog.tableExists(TARGET_TABLE):
            logger.info("Target table not found. Starting from scratch.")
            return {}
        
        # Get the max date per country from the table
        df = self.spark.table(TARGET_TABLE) \
            .groupBy("country") \
            .agg(spark_max("date").alias("max_date"))
        
        return {row.country: row.max_date for row in df.collect()}

# COMMAND ----------

# orchestration
class WeatherETL:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.state_manager = StateManager(spark)
        self.client = WeatherClient()

    def run(self):
        # Load locations from Volume
        locations_df = self.spark.read.csv(REF_PATH, header=True, inferSchema=True)
        locations = locations_df.filter("latitude IS NOT NULL").collect()

        # Get Watermarks (e.g., will find 2026-07-31)
        last_dates = self.state_manager.get_last_ingested_dates()

        all_rows = []
        ingestion_ts = datetime.now(timezone.utc)
        
        # Today is Aug 28, 2026. Archive API is finalized up to ~3 days ago.
        latest_pull_date = (datetime.now(timezone.utc) - timedelta(days=3)).date()

        for row in locations:
            country = row.name
            last_date_val = last_dates.get(country)
            
            # Watermark Logic: Use last date from table minus 7 day overlap for corrections
            if last_date_val:
                if isinstance(last_date_val, str):
                    start_dt = datetime.strptime(last_date_val, "%Y-%m-%d").date() - timedelta(days=7)
                else:
                    start_dt = last_date_val - timedelta(days=7)
            else:
                start_dt = (datetime.now(timezone.utc) - timedelta(days=30)).date()
            
            # Skip if we are already current
            if start_dt >= latest_pull_date:
                continue

            start_date_str = start_dt.strftime("%Y-%m-%d")
            end_date_str = latest_pull_date.strftime("%Y-%m-%d")

            data = self.client.fetch_with_retry(row.latitude, row.longitude, start_date_str, end_date_str, country)
            
            if data and "daily" in data:
                d = data["daily"]
                times = d.get("time", [])
                if times:
                    logger.info(f"✅ {country}: Fetched {len(times)} days ({start_date_str} to {end_date_str})")
                    for i in range(len(times)):
                        all_rows.append(Row(
                            country=country, 
                            date=times[i], 
                            temperature_2m_max=float(d["temperature_2m_max"][i]) if d["temperature_2m_max"][i] is not None else None, 
                            temperature_2m_min=float(d["temperature_2m_min"][i]) if d["temperature_2m_min"][i] is not None else None, 
                            latitude=float(row.latitude), 
                            longitude=float(row.longitude), 
                            ingested_at=ingestion_ts
                        ))
            time.sleep(0.2)

        if all_rows:
            self.save_to_delta(all_rows)
        else:
            logger.info("🏁 Pipeline Finished: Table is already up to date for Aug 2026.")

    def save_to_delta(self, rows):
        """Idempotent save using programmatic Delta Merge."""
        # Convert list of Rows to DataFrame
        new_df = self.spark.createDataFrame(rows)
        
        if not self.spark.catalog.tableExists(TARGET_TABLE):
            logger.info(f"Creating new table {TARGET_TABLE}")
            new_df.write.format("delta").saveAsTable(TARGET_TABLE)
        else:
            logger.info(f"Merging {len(rows)} records into {TARGET_TABLE}")
            # Programmatic Delta Merge to ensure no duplicates
            dt = DeltaTable.forName(self.spark, TARGET_TABLE)
            dt.alias("t").merge(
                source=new_df.alias("s"),
                condition="t.country = s.country AND t.date = s.date"
            ).whenMatchedUpdate(set={
                "temperature_2m_max": "s.temperature_2m_max",
                "temperature_2m_min": "s.temperature_2m_min",
                "ingested_at": "s.ingested_at"
            }).whenNotMatchedInsertAll().execute()
            logger.info("🏁 Merge Successful.")

# COMMAND ----------

# execution
if __name__ == "__main__":
    WeatherETL(spark).run()