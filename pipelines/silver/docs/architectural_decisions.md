# Architecture Decision Records

## 1: Relational Normalisation (Pivot-to-Long)
*   **Context:** Source data (FAO/OWID) arrives in "Wide" formats with years as columns.
*   **Decision:** All annual data is restructured into a "Long" format (one row per year, per metric).
*   **Pros:** Facilitates efficient time-series analysis; ensures schema stability when new years are added.
*   **Cons:** Increases total row count.
*   **Alternative:** Maintaining "Wide" tables. *Rejected:* Incompatible with daily weather joins.
*   **Standard Practice:** Yes. This follows the 'Tidy Data' standard essential for BI and AI processing.

## 2: Stateful Batch Processing (Watermarking)
*   **Context:** Bronze data is populated via manual uploads and irregular API calls.
*   **Decision:** Use the `ingested_at` timestamp as a high-water mark for incremental promotion to Silver.
*   **Pros:** Prevents redundant processing; ensures cluster stability on the Free Edition.
*   **Cons:** Higher latency than real-time streaming (not required for this use case).
*   **Alternative:** Full Table Overwrite. *Rejected:* Wasteful of compute credits.

## 3: Data Validation & Integrity Management
*   **Context:** Sensor data often contains erroneous coordinates (e.g., 0,0 in the ocean).
*   **Decision:** Implement a 'Land Mask' flag. Data not falling on known landmasses is flagged (`is_on_land = False`) and excluded from primary dashboards.
*   **Pros:** Prevents "Ocean Weather" from corrupting climate insights while preserving raw data for audit.
*   **Cons:** Requires an additional spatial lookup during ingestion.
*   **Alternative:** Hard Deletion. *Rejected:* Prevents future correction of sensor errors.

## 4: Metadata-Driven State Management
*   **Context:** Incremental pipelines require a way to remember which data has already been processed.
*   **Decision:** We use a physical Delta table (`climate_energy_demand.silver.ingestion_audit`) to store high-water marks (timestamps) for every Silver table.
*   **Location:** Created by `pipelines/silver/setup_silver.sql`
*   **Schema:** `table_name STRING, last_watermark TIMESTAMP, rows_processed INT, processed_at TIMESTAMP`
*   **Pros:** 
    *   **Transparency:** The AI/BI Genie and human auditors can query the table to see data freshness.
    *   **Persistence:** Unlike local memory, the state is preserved if a cluster fails.
*   **Cons:** Requires a "Bootstrap" step to create the table before pipelines can run.
*   **Alternative:** Spark Checkpoints. *Rejected:* Checkpoint files are opaque and cannot be easily queried by BI tools.

## 6: Config-Driven Orchestration
*   **Context:** Silver layer processes multiple tables with similar patterns but different sources and transformations.
*   **Decision:** Use YAML configs (`pipelines/silver/configs/*.yml`) to define each table's sources, transform module, merge keys, and watermark column.
*   **Orchestrator:** `pipelines/silver/silver_orchestrator.py` iterates through all configs and executes the ETL pattern.
*   **Pros:**
    *   **Separation of Concerns:** Table definitions (YAML) separate from orchestration logic (Python).
    *   **Maintainability:** Adding a new Silver table requires only a new YAML file, no code changes.
    *   **Auditability:** Config files are version-controlled and human-readable.
*   **Cons:** Requires discipline to keep YAML schema consistent.
*   **Alternative:** Hard-coded tables in orchestrator. *Rejected:* Not scalable; every new table requires code modification.


## 5. Why use SQL over Python for setup?
SQL is preferred for setup for three reasons:
* **Readability**: It is the native language for Data Definition (DDL). A "cold reader" can immediately understand the table structure without parsing Python logic.
* **Genie Compatibility**: Databricks SQL allows for inline COMMENT ON statements which are the primary ways to "teach" the AI/BI Genie what the data means.
* **Idempotency**: SQL CREATE TABLE IF NOT EXISTS is the standard for Infrastructure as Code (IaC).


### Others
Key Engineering Decisions in this file:
Dual-Source Orchestration:

Unlike the previous YAMLs, `pipelines/silver/configs/forest_inventory_annual.yml`  lists two sources. two sources to create a single clean output. It joins the raw FAO metrics with the item code definitions to ensure that the Silver layer contains human-readable terms like "Forest land" instead of numeric codes.
The orchestrator will be designed to load both into a dictionary and pass them to the process_forest_inventory function. This ensures the "Logic" (the join) remains in Python, while the "Input" (the table names) remains in YAML.

Granular merge_keys:
Because one country will have multiple rows for different land types (e.g., "Forest land", "Cropland") for the same year, we include item in the merge keys. This ensures the Delta MERGE operation is precise and doesn't accidentally overwrite "Forest" data with "Crop" data.

Terminology Alignment:
used country_name in the merge_keys to match the output of your process_forest_inventory Python function. Consistency between the YAML and the Python code is vital for a "Cold Reader" to follow the data lineage.

Unit Standardisation Placeholder:
added unit_standard: "ha" to the parameters. This acts as a clear signal that the Silver layer is responsible for ensuring all nature data is in Hectares, making it instantly joinable with the Carbon Flux data later.



`pipelines/silver/configs/energy_metrics.yml` table is particularly important because it provides the socio-economic context (GDP and Population) that the AI/BI Genie will use to normalise climate impacts (e.g., "Energy demand per capita").
Key Engineering Decisions in this file:
Composite Primary Key (merge_keys):
Energy data is annual. By using iso_code and year, we ensure that the Silver layer always has exactly one row per country, per year. This makes joining with the Forestry data later very simple.

Contextual Description:
The description specifically mentions "GDP" and "Population." This is a "Cold Reader" best practice. It ensures an analyst knows they don't need to look for a separate "Economy" table—it's already integrated here.

Parameterised Filtering:
added start_year: 2010 to the params. This allows you to control the temporal scope of the whole project from the config file. If you ever decide to expand the project back to the year 2000, you just change this number; the Python logic remains untouched.

ISO-3166 Standardisation:
The use of iso_code as a merge key enforces a global standard. This ensures that "France" in the Energy table always matches "FRA" in the Weather table.



`pipelines/silver/configs/weather_historical.yml`
Logical Grouping: separated Target, Sources, Logic, State, and Params. This makes it easy to see where the "Infrastructure" ends and the "Science" begins.

Clear Terminology:
Instead of just keys, used merge_keys. This tells a developer that this table uses Upsert logic rather than just Appending data.
watermark_column clearly signals that this table supports incremental loading.

Genie Hooks: The description field is phrased as a full sentence. This is best practice for the AI/BI Genie, which uses these descriptions to decide which table to use for natural language queries.




Global Forest Watch (GFW) data. It is the most technically distinct of your nature datasets because it moves from raw geographical points (Latitude/Longitude) to a structured Geospatial Index (H3).
`pipelines/silver/configs/carbon_flux_spatial.yml`
Key Engineering Decisions in this file:
Spatial-Temporal Merge Keys:
By using h3_cell and year as the primary keys, we ensure that each 30km hexagon on Earth has exactly one "Carbon Profile" per year. This prevents the Silver layer from becoming cluttered with overlapping or duplicate spatial tiles.

H3 Resolution Standardisation:
We have explicitly set h3_resolution: 6. This ensures that the carbon data is "binned" at the exact same spatial grain as your weather data, making the Gold-layer join a simple ID-to-ID match.

Genie-Optimised Description:
The description specifically mentions "Sink vs Source." This tells the AI that this table is the "Source of Truth" for determining if a forest is capturing carbon or losing it to the atmosphere.

Decoupling Logic from Space:
in the future if Resolution 6 is too coarse, only change this single number in the YAML. The Python logic will automatically recalculate the entire grid at the new resolution during the next run.



`pipelines/silver/configs/dim_locations.yml` defines the Master Location Lookup. It is one of the most critical files in the repository because it creates the "Common Denominator" (ISO Alpha-3 codes) that allows the weather data to be joined with the energy and nature data.

Key Engineering Decisions in this file:
Enforcing the "Single Source of Truth":
By joining FAO and OWID in this specific YAML, we resolve naming discrepancies (e.g., "Czech Republic" vs "Czechia") at the Silver gate. Every downstream table will now use the iso_code from this master list.

The "Common" Module:
assigned this to the common module. This is best practice for dimensions that are used by multiple different pipelines (Weather, Energy, and Nature).

Strict merge_keys:
only use iso_code as the merge key. This ensures that the dimension table never contains duplicate entries for the same country, even if the source data is updated multiple times.

Genie Support:
The description explicitly mentions "ISO-3166 Alpha-3." This allows the AI/BI Genie to understand that this table is the "Map" it should use when a user asks a question about a specific region or country.


The best practice is to create `pipelines/silver/configs/dim_date.yml` once in Silver and reference it in Gold.
Why Silver? The Silver layer is the "Standardisation" gate. By creating the dim_date here, you ensure that every standardisation script (like your Weather script) has a "spine" to join against immediately.

Why not repeat it in Gold? Repeating the code in Gold is a "DRY" (Don't Repeat Yourself) violation. If you decide in six months to add "EU Public Holidays" to your calendar, you would have to update two scripts. If you build it in Silver, the Gold layer automatically sees the new columns.

Recommendation: Keep the YAML and the logic in the Silver pipeline. The Gold layer will treat it as a "read-only" source.
sources: {}: This is a clear signal to an auditor that this table is "System Generated." It prevents them from wasting time looking for a bronze.date table that doesn't exist.

Seasonality Logic: By including "EU Seasonality" in the description, you tell the AI/BI Genie that it can answer questions like "Show me energy demand for all Winters since 2020."

Extended Range: By extending the calendar to 2040, you ensure the Gold layer can handle the Climate Projections data without the joins failing for future dates.


`pipelines/silver/configs/dim_h3_grid.yml` configuration creates the Master Spatial Dimension. It is a critical "bridge" table that classifies every 30km hexagon (H3 Resolution 6) by its land type. This allow the AI to answer questions like: "What was the temperature stress in Peatland regions compared to Urban areas?"

Key Engineering Decisions in this file:
Spatial Classification (The "Why"):
Standard Latitude/Longitude points are useless for the AI/BI Genie because it cannot "reason" about coordinates. By classifying hexagons as "Peatland" or "Urban," we give the AI the vocabulary it needs to perform geographical comparisons.

H3 Resolution Parity:
By forcing h3_resolution: 6 here, we guarantee that this dimension table will "snap" perfectly to your weather and carbon flux tables. This avoids the "spatial mismatch" problem common in many climate projects.

Composite Primary Key:
The merge_keys include land_type. This is a professional safeguard. If a single H3 hexagon contains both "Peatland" and "Forest," this structure allows us to capture both attributes rather than overwriting one with the other.

Decoupling Logic:
The transformation logic (the geospatial module) handles the heavy geometry math, while the YAML simply defines the input and the grain. This makes the pipeline very easy to maintain if you acquire better land-type data (e.g., Urban heat-island maps) in the future.


`pipelines/silver/configs/dim_stations.yml` configuration extracts and standardises the metadata for the NOAA weather stations. It is a critical "bridge" table that ensures every weather station is correctly mapped to a specific Country and H3 Hexagon, allowing for seamless joining between station-level ground truths and national-level energy data.

Key Engineering Decisions in this file:
Deduplication at the Source:
In the Bronze layer, station metadata is often repeated across millions of rows of daily observations. This YAML triggers a "Distinct" extraction, creating a lean, high-performance lookup table that reduces data redundancy in the Silver layer.

Spatial Anchoring:
By including h3_resolution: 6 in the parameters, the logic will assign each station to an H3 cell. This allows you to say: "Station X is the primary ground-truth for Hexagon Y," which is essential for validating your climate models later.

Data Integrity:
The merge_keys is set strictly to station_id. This prevents the dimension table from growing indefinitely if station coordinates are updated; instead, the existing record is updated with the most recent, accurate location.

Genie Searchability:
The description specifically mentions "National Boundaries." This signals to the AI/BI Genie that this table can be used to filter or group stations by country, answering questions such as: "How many active weather stations are contributing to the Singapore energy model?"


The files below complete your metadata blueprint. They handle the "Ground Truth" (what actually happened) and the "Projections" (what might happen in the future), providing the final data points needed for your climate risk models.

`pipelines/silver/configs/weather_observations.yml`
This configuration handles the NOAA GSOD data. Its primary purpose is the conversion of raw "Imperial" units (Fahrenheit, Inches) into the metric standards (Celsius, Millimetres) required for scientific consistency across the project.

`pipelines/silver/configs/weather_projections.yml`
This configuration processes the CMIP6 Climate Projections. It is vital for "Forward-Looking" analytics, allowing the platform to compare current energy demand against future heatwave or cold-snap scenarios.
Key Engineering Decisions for these files:
Multi-Model Integrity (weather_projections):
Unlike historical data, projections come from different scientific models. By including model in the merge_keys, the Silver layer can store several versions of the future (e.g. a "Best Case" vs. a "Worst Case" scenario) without them overwriting each other.

Imperial-to-Metric Standardisation (weather_observations):
The YAML flags the unit_standard as "metric". This ensures that the Python logic in the weather module performs the conversion (F-32) * 5/9. This is the "Data Integrity" gate that prevents mixing Celsius and Fahrenheit in your dashboards.

Temporal Consistency:
Both files use the date column as a merge key. This allows for a clean join against the dim_date table we created earlier, enabling the Genie to perform time-series comparisons between historical "Ground Truth" and future "Projections."


`pipelines/silver/silver_orchestrator.py` uses Delta Merge logic to ensure that your manual Bronze uploads never create duplicate records in Silver. This is the "Engine" that reads your metadata and executes the transformations.
Key Engineering Principles in the Orchestrator:
Dynamic Import (importlib):
The engine doesn't know about "Weather" or "Energy." It simply reads the module and function strings from the YAML and fetches the code. This makes the system infinitely scalable—you can add a new domain just by adding a new Python file in transforms/.

SQL-Based Delta Merge:
On the Free Edition, the Python DeltaTable API can sometimes be slow. We generate a SQL Merge string dynamically using the merge_keys from your YAML. This is the most efficient way to prevent duplicate rows.

State Resilience:
By using the try/except block inside the loop, the orchestrator is "fault-tolerant." If the NOAA weather data has a schema error, the script will skip it and move on to process the FAO data, ensuring you get the most work done possible within your 2-hour cluster limit.

Genie Integration:
The orchestrator automatically applies the description from the YAML as a COMMENT ON TABLE. This ensures that your AI/BI Genie always has the metadata it needs to answer user questions.

Summary of what we have achieved:
Decoupled Logic: The "Engine" is separate from the "Maths."

Metadata-Driven: You control everything via 10 simple YAML files.

Incremental: You only process new data, respecting the Free Edition's limits.


`databricks.yml` is the final "Infrastructure as Code" (IaC) component. The databricks.yml file acts as the blueprint for your entire project, telling the Databricks platform how to deploy and run your modular library on the Free Edition.
Idempotent Orchestration:
By putting the setup_silver.sql and silver_orchestrator.py in the same Job, you ensure that every time the pipeline runs, it first verifies that the tables exist. This makes the system "self-healing."

Single-Node Optimization:
The configuration num_workers: 0 is the specific "Free Edition" flag. It ensures you don't try to spin up a multi-node cluster that the Community Edition would reject.

Path Resolution:
In the September 2026 environment, Databricks Asset Bundles automatically manage the "Workspace Files" for you. When you deploy this bundle, the Python script will be able to find the configs/ folder and the src/ library using the relative paths we built into the orchestrator.

Scaling to Paid Tier:
If you move to a Paid Tier later, you only change one line in this file (num_workers: 8) and the entire project scales to a massive cluster without touching any Python code.



We have now designed the following:

Repository Structure: Modular, professional, and jargon-free.

Infrastructure: SQL-based Catalog, Schemas, and Audit Table.

Metadata: 10 YAML files defining the "Contract" for each table.

Logic: 5 Python modules in src/transforms/ handling the math.

The Engine: A Python Orchestrator that automates everything.

IaC: A databricks.yml to deploy and schedule the work.

How to start the project:
Deploy: Run databricks bundle deploy from your terminal.

Initialise: Run the setup/project_infrastructure.sql once manually in the SQL Editor to create the Catalog.

Execute: Start the "Silver Layer: Master Orchestrator" job in the Databricks UI.

This concludes the Silver Layer Design & Engineering Phase.