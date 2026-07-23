# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Open-Meteo CMIP6 Climate Projections Ingestion
# MAGIC Pulls CMIP6 climate model projections for the 5 Phase 1 countries across all 7
# MAGIC available climate models, for a sample of future years (2020-2050, every 5th year),
# MAGIC and writes raw results to a bronze Delta table.
# MAGIC
# MAGIC This hits a different Open-Meteo endpoint than `01_ingest_openmeteo_historical.py` --
# MAGIC the Climate API, not the Historical Weather API. It has no SSP2-4.5/SSP5-8.5 scenario
# MAGIC switch; instead it offers 7 climate models, and the spread across them is what stands
# MAGIC in for scenario uncertainty here.
# MAGIC
# MAGIC Only whole years are pulled (Jan-Dec) rather than the full 1950-2050 continuous range,
# MAGIC to stay well within Open-Meteo's free-tier rate limits -- each sampled year still gets
# MAGIC a real, correct annual HDD/CDD total; years in between are just not pulled at all.
# MAGIC
# MAGIC Safe to rerun: only overwrites rows for the sampled years (see `replaceWhere` below).
# MAGIC Can also be run in steps -- each year fetches and writes on its own, so you can do
# MAGIC a few sample years now and the rest later without losing anything.
# MAGIC Assumes the catalog/schemas already exist from `01_ingest_openmeteo_historical.py`.
# MAGIC
# MAGIC API: https://open-meteo.com/en/docs/climate-api

# COMMAND ----------

import requests
import time
from datetime import datetime, UTC
from pyspark.sql import Row

# COMMAND ----------

# Representative coordinates per country (capital cities) -- same as
# 01_ingest_openmeteo_historical.py
COUNTRIES = {
    "France": (48.8566, 2.3522),
    "Germany": (52.5200, 13.4050),
    "Spain": (40.4168, -3.7038),
    "Italy": (41.9028, 12.4964),
    "United Kingdom": (51.5074, -0.1278),
}

# Whole calendar years to sample, instead of a full 1950-2050 continuous pull.
SAMPLE_YEARS = [2020, 2025, 2030, 2035, 2040, 2045, 2050]

# All 7 available climate models -- kept complete, since the spread across them is
# the actual uncertainty signal (this API has no SSP2-4.5/SSP5-8.5 scenario picker).
MODELS = [
    "CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S",
    "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S",
]

DAILY_VARS = "temperature_2m_mean,temperature_2m_max,temperature_2m_min"

CLIMATE_URL = "https://climate-api.open-meteo.com/v1/climate"

# COMMAND ----------

def fetch_country_year(country: str, lat: float, lon: float, year: int) -> dict:
    """Call the Open-Meteo climate API for one country's coordinates, one full year, all models."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "models": ",".join(MODELS),
        "daily": DAILY_VARS,
    }
    response = requests.get(CLIMATE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check column naming before the full pull
# MAGIC Confirms how Open-Meteo names columns when multiple models are requested at once.
# MAGIC Expected pattern: `{variable}_{model}`, e.g. `temperature_2m_mean_CMCC_CM2_VHR4` --
# MAGIC this hasn't been verified against a live call, so check it here first.

# COMMAND ----------

_sample = fetch_country_year("France", *COUNTRIES["France"], 2030)
print(list(_sample["daily"].keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch + write, one year at a time
# MAGIC Each sample year fetches all 5 countries, then writes immediately -- scoped to just
# MAGIC that year via `replaceWhere` -- before moving to the next. That means this can be done
# MAGIC in steps: run this cell now for some years, later trim `SAMPLE_YEARS` down to whatever's
# MAGIC left and rerun -- nothing already written gets touched either way.

# COMMAND ----------

target_table = "climate_energy_demand.bronze.openmeteo_cmip6_projections"
ingestion_ts = datetime.now(UTC).isoformat()

for year in SAMPLE_YEARS:
    year_rows = []

    for country, (lat, lon) in COUNTRIES.items():
        print(f"Fetching {country} / {year}...")
        data = fetch_country_year(country, lat, lon, year)
        daily = data["daily"]
        dates = daily["time"]

        for model in MODELS:
            temp_mean = daily.get(f"temperature_2m_mean_{model}")
            temp_max = daily.get(f"temperature_2m_max_{model}")
            temp_min = daily.get(f"temperature_2m_min_{model}")
            if temp_mean is None:
                print(f"WARNING: no columns found for model {model} -- check naming pattern above")
                continue

            for d, tmean, tmax, tmin in zip(dates, temp_mean, temp_max, temp_min):
                year_rows.append(
                    Row(
                        country=country,
                        date=d,
                        model=model,
                        temperature_2m_mean=tmean,
                        temperature_2m_max=tmax,
                        temperature_2m_min=tmin,
                        latitude=lat,
                        longitude=lon,
                        ingested_at=ingestion_ts,
                    )
                )

        # Wider pause than the historical notebook's 1s -- each call here is heavier
        # (full year x 7 models x 3 variables), and Open-Meteo's per-minute limit may
        # weight requests by size rather than counting them 1-for-1.
        time.sleep(5)

    # Write this year immediately, scoped to just this year -- other sampled years,
    # whether already written in a past run or not yet fetched, are untouched.
    year_df = spark.createDataFrame(year_rows)
    if spark.catalog.tableExists(target_table):
        (
            year_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"YEAR(date) = {year}")
            .saveAsTable(target_table)
        )
    else:
        year_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    print(f"Wrote {len(year_rows)} rows for {year} to {target_table}")

print("Done with this batch of sample years.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, model, COUNT(*) AS days, MIN(date) AS first_date, MAX(date) AS last_date
# MAGIC FROM climate_energy_demand.bronze.openmeteo_cmip6_projections
# MAGIC GROUP BY country, model
# MAGIC ORDER BY country, model

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE TABLE climate_energy_demand.bronze.openmeteo_historical;
# MAGIC -- DESCRIBE TABLE climate_energy_demand.bronze.noaa_gsod;
# MAGIC -- DESCRIBE TABLE climate_energy_demand.bronze.owid_energy;
# MAGIC -- DESCRIBE TABLE climate_energy_demand.bronze.openmeteo_cmip6_projections;