# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Our World in Data — Energy Consumption Ingestion
# MAGIC Reads OWID's energy dataset (uploaded manually to a Volume — see next
# MAGIC cell) and writes the filtered subset for the 5 Phase 1 countries to a
# MAGIC bronze Delta table.
# MAGIC
# MAGIC Source: https://github.com/owid/energy-data
# MAGIC Updated annually — safe to simply overwrite on each run, no watermark needed.
# MAGIC To refresh: re-download the CSV and re-upload it to the Volume, overwriting
# MAGIC the existing file, then rerun this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Free Edition serverless compute blocked outbound requests to
# MAGIC both the OWID and GitHub domains for this file (DNS resolution failed —
# MAGIC likely a restricted egress allowlist Free Edition users can't self-manage).
# MAGIC As a workaround, the CSV is downloaded manually and uploaded to a Unity
# MAGIC Catalog Volume, then read from there as a local path instead of over HTTP.
# MAGIC
# MAGIC One-time setup (run in a SQL cell before this notebook):
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS climate_energy_demand.bronze.raw_uploads;
# MAGIC ```
# MAGIC Then upload `owid-energy-data.csv` via Catalog → climate_energy_demand →
# MAGIC bronze → raw_uploads → "Upload to this volume".

# COMMAND ----------

import pandas as pd

OWID_PATH = "/Volumes/climate_energy_demand/bronze/raw_uploads/owid-energy-data.csv"

COUNTRIES = ["France", "Germany", "Spain", "Italy", "United Kingdom"]
START_YEAR = 2010

COLUMNS = [
    "country",
    "year",
    "iso_code",
    "population",
    "electricity_demand",
    "electricity_generation",
    "primary_energy_consumption",
]

# COMMAND ----------

pdf = pd.read_csv(OWID_PATH, usecols=COLUMNS)

pdf = pdf[pdf["country"].isin(COUNTRIES) & (pdf["year"] >= START_YEAR)].reset_index(drop=True)

print(f"Rows after filtering to 5 countries, {START_YEAR}+: {len(pdf)}")
pdf.head()

# COMMAND ----------

df = spark.createDataFrame(pdf)
display(df.limit(5))

# COMMAND ----------

target_table = "climate_energy_demand.bronze.owid_energy"

df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"Written to table: {target_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, COUNT(*) AS years, MIN(year) AS first_year, MAX(year) AS last_year
# MAGIC FROM climate_energy_demand.bronze.owid_energy
# MAGIC GROUP BY country