# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Forestry, Land Cover, and Carbon Flux Ingestion (FAO & GFW)
# MAGIC
# MAGIC This notebook performs a bulk ingestion of high-resolution forestry and environmental datasets 
# MAGIC from FAOSTAT and Global Forest Watch. It follows the "Raw Mirror" pattern, where every source 
# MAGIC CSV is converted 1-to-1 into a Bronze Delta table with zero filtering or transformations.
# MAGIC
# MAGIC ### Sources
# MAGIC * **FAOSTAT (UN):** Land Use (RL), Land Cover (LC), and Temperature Change (ET) domains.
# MAGIC * **Global Forest Watch (GFW):** Carbon Net Flux, Emissions Polygons, Tropical Tree Cover, 
# MAGIC   and Global Peatlands.
# MAGIC
# MAGIC ### Logic & Maintenance
# MAGIC * **Egress Workaround:** As with the OWID and NOAA datasets, these files are manually 
# MAGIC   downloaded and uploaded to a Unity Catalog Volume to bypass Databricks Free Edition 
# MAGIC   network restrictions.
# MAGIC * **Encoding:** FAO datasets are ingested using `ISO-8859-1` encoding to correctly 
# MAGIC   preserve special characters in international area names.
# MAGIC * **No Filtering:** Unlike Phase 1, this notebook ingests the full global datasets. 
# MAGIC   Filtering for specific countries (Nordics, Singapore, etc.) is deferred to the Silver layer 
# MAGIC   to ensure the Bronze layer remains a robust, reusable raw archive.
# MAGIC * **Lineage:** Every record is enriched with `ingested_at` (timestamp) and `source_file` 
# MAGIC   (path) metadata.
# MAGIC
# MAGIC ### Setup
# MAGIC All CSV files must be present in:
# MAGIC `/Volumes/climate_energy_demand/bronze/raw_uploads/`
# MAGIC
# MAGIC These are ingested from a Unity Catalog Volume to  bypass egress restrictions on the Databricks Free Edition. No transformations or joins are performed at this stage; relational mapping is deferred to Silver.
# MAGIC To upload the files via Catalog → climate_energy_demand → bronze → raw_uploads → "Upload to this volume".

# COMMAND ----------

import re
from pyspark.sql.functions import col, current_timestamp, input_file_name

# COMMAND ----------

# --- CONFIGURATION ---
CATALOG = "climate_energy_demand"
SCHEMA = "bronze"
# Update this to match your Volume path
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_uploads"

# COMMAND ----------

# --- HELPER: Cleans column names ---
def sanitize_column_name(name):
    """
    Cleans column names for Delta compatibility:
    - Replaces spaces and special chars with underscores
    - Converts to lowercase
    - Removes trailing underscores
    """
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
    return re.sub(r'_+', '_', clean_name).strip('_')

# COMMAND ----------

# --- HELPER: Determine Table Name from Filename ---
def get_target_table_name(filename):
    fn = filename.lower()
    if "inputs_landuse" in fn: base = "fao_land_use"
    elif "environment_landcover" in fn: base = "fao_land_cover"
    elif "environment_temperature_change" in fn: base = "fao_temp_change"
    elif "forest_greenhouse" in fn: return fn.replace("forest_greenhouse_gas_", "gfw_").replace(".csv", "")
    elif "global_peatlands" in fn: return "gfw_peatlands"
    elif "tropical_tree_cover" in fn: return "gfw_tropical_tree_cover"
    else: return None

    if "data_noflag" in fn: sub = "data_noflag"
    elif "all_data" in fn: sub = "all_data"
    elif "areacodes" in fn: sub = "area_codes"
    elif "itemcodes" in fn: sub = "item_codes"
    elif "elements" in fn: sub = "elements"
    elif "flags" in fn: sub = "flags"
    else: sub = "raw"
    return f"{base}_{sub}"

# COMMAND ----------

# --- INGESTION LOGIC ---
files = [f for f in dbutils.fs.ls(VOLUME_PATH) if f.name.endswith(".csv")]

for file in files:
    table_name = get_target_table_name(file.name)
    
    if table_name:
        print(f"Ingesting: {file.name} -> {table_name}")
        
        encoding = "ISO-8859-1" if "fao_" in table_name else "UTF-8"
        
        # 1. Read
        df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("encoding", encoding) 
              .load(file.path))

        # 2. Sanitize Column Names (Fixes the DELTA_INVALID_CHARACTERS error)
        clean_columns = [sanitize_column_name(c) for c in df.columns]
        df = df.toDF(*clean_columns)

        # 3. Add Lineage Metadata (UC Modern way)
        df_final = (df.withColumn("ingested_at", current_timestamp())
                      .select("*", col("_metadata.file_path").alias("source_file")))

        # 4. Write to Delta
        (df_final.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))
    else:
        print(f"Skipped: {file.name}")