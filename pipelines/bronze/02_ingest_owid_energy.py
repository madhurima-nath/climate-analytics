# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Our World in Data — Energy Consumption Ingestion
# MAGIC Reads the full OWID global energy dataset (uploaded manually to a Volume) 
# MAGIC and writes it to a bronze Delta table. 
# MAGIC
# MAGIC Source: https://github.com/owid/energy-data
# MAGIC Updated annually — safe to simply overwrite on each run, no watermark needed.
# MAGIC
# MAGIC To refresh: re-download the CSV and re-upload it to the Volume, overwriting 
# MAGIC the existing file, then rerun this notebook.
# MAGIC
# MAGIC Databricks Free Edition serverless compute blocked outbound requests to both 
# MAGIC the OWID and GitHub domains for this file. As a workaround, the CSV is 
# MAGIC downloaded manually and uploaded to a Unity Catalog Volume.

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

from pyspark.sql.functions import current_timestamp, col
import re

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads/owid-energy-data.csv"

# COMMAND ----------

# COLUMNS = [
#     "country",
#     "year",
#     "iso_code",
#     "population",
#     "electricity_demand",
#     "electricity_generation",
#     "primary_energy_consumption",
# ]

# COMMAND ----------

# --- HELPER: SANITIZE COLUMN NAMES ---
def sanitize_column_name(name):
    # Ensures Delta compatibility (removes spaces/special chars)
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
    return re.sub(r'_+', '_', clean_name).strip('_')

# COMMAND ----------

# --- INGESTION ---
print(f"Reading raw data from {VOLUME_PATH}...")

# Read the raw CSV - using inferSchema to get correct types for the 100+ metrics
df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(VOLUME_PATH))

# COMMAND ----------

# Sanitize headers (important if the CSV has trailing spaces or weird chars)
clean_columns = [sanitize_column_name(c) for c in df.columns]
df = df.toDF(*clean_columns)

# Standard Bronze Metadata (Lineage) using UC-compliant _metadata
df_final = (df.withColumn("ingested_at", current_timestamp())
              .select("*", col("_metadata.file_path").alias("source_file")))

# COMMAND ----------

# Write to Bronze
# NOTE: No filter() applied. Ingesting all columns and all countries.
(df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.owid_energy"))

print(f"Success. Ingested {df_final.count()} rows and {len(df_final.columns)} columns.")