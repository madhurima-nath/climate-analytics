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

### Watermarking Flow

```
First Run (Cold Start):
┌────────────────────────────────────────────────────────────────┐
│  get_last_watermark('silver.weather_historical')               │
│  └─> No record found                                           │
│  └─> Returns: 1900-01-01 00:00:00 (epoch default)             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Extract from Bronze:                                          │
│  SELECT * FROM bronze.weather_raw                              │
│  WHERE ingested_at > '1900-01-01'                              │
│  └─> All data (full historical load)                           │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Transform & Load: 10,000 rows → silver.weather_historical     │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  update_audit_log(                                             │
│    table_name='silver.weather_historical',                     │
│    last_watermark='2024-01-15 12:00:00',  ← max(ingested_at)  │
│    rows_processed=10000                                        │
│  )                                                             │
└────────────────────────────────────────────────────────────────┘


Subsequent Run (Incremental):
┌────────────────────────────────────────────────────────────────┐
│  get_last_watermark('silver.weather_historical')               │
│  └─> Record found                                              │
│  └─> Returns: 2024-01-15 12:00:00                             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Extract from Bronze:                                          │
│  SELECT * FROM bronze.weather_raw                              │
│  WHERE ingested_at > '2024-01-15 12:00:00'                     │
│  └─> Only new data (100 rows)                                  │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Transform & MERGE: 100 rows → silver.weather_historical       │
│  (Upsert based on merge_keys: [station_id, date])             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  update_audit_log(                                             │
│    table_name='silver.weather_historical',                     │
│    last_watermark='2024-01-20 09:00:00',  ← new max()          │
│    rows_processed=100                                          │
│  )                                                             │
└────────────────────────────────────────────────────────────────┘
```

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

## 5: Why SQL for Infrastructure Setup

SQL is used for the bootstrap and setup scripts for three reasons:
*   **Readability:** SQL is the native language for data definition. A reader can understand the table structure directly, without parsing Python logic.
*   **Genie compatibility:** Databricks SQL supports inline `COMMENT ON` statements, which are the primary way to teach AI/BI Genie what the data means.
*   **Idempotency:** `CREATE TABLE IF NOT EXISTS` is the standard Infrastructure as Code pattern, so the bootstrap can run repeatedly without side effects.

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

### Config-Driven Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  pipelines/silver/configs/                                      │
│  ├── weather_historical.yml                                     │
│  ├── energy_metrics.yml                                         │
│  ├── forest_inventory_annual.yml                                │
│  ├── temp_change_annual.yml                                     │
│  ├── dim_locations.yml                                          │
│  ├── dim_date.yml                                               │
│  └── ... (10 configs total)                                     │
└────────────┬────────────────────────────────────────────────────┘
             │
             │  Each YAML defines:
             │  • target_table
             │  • sources (Bronze tables)
             │  • module & function (Transform)
             │  • merge_keys (for UPSERT)
             │  • watermark_column (for incremental)
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  silver_orchestrator.py                                         │
│                                                                 │
│  for config_file in configs/*.yml:                              │
│    cfg = yaml.safe_load(config_file)                            │
│    ┌──────────────────────────────────┐                        │
│    │ Generic ETL Pattern:             │                        │
│    │ 1. get_last_watermark()          │                        │
│    │ 2. extract_sources()             │                        │
│    │ 3. transform(sources, params)    │                        │
│    │ 4. merge_or_create()             │                        │
│    │ 5. update_audit_log()            │                        │
│    └──────────────────────────────────┘                        │
└────────────┬────────────────────────────────────────────────────┘
             │
             │  Transform logic in:
             ▼
┌─────────────────────────────────────────────────────────────────┐
│  src/transforms/                                                │
│  ├── weather.py                                                 │
│  │   └── process_weather_historical()                           │
│  ├── energy.py                                                  │
│  │   └── process_energy_metrics()                               │
│  ├── nature.py                                                  │
│  │   └── process_forest_inventory()                             │
│  └── common.py                                                  │
│      ├── create_dim_locations()                             │
│      ├── create_dim_date()                                  │
                             │
└─────────────────────────────────────────────────────────────────┘

  Adding a New Table = Creating One YAML File
  ┌────────────────────────────────────────┐
  │ new_table.yml                          │
  │ ───────────────                        │
  │ target_table: silver.new_table         │
  │ sources:                               │
  │   primary: bronze.new_source           │
  │ module: transforms.new_module          │
  │ function: process_new_data             │
  │ merge_keys: [id, date]                 │
  │ watermark_column: ingested_at          │
  └────────────────────────────────────────┘
           │
           └─> Orchestrator auto-discovers and processes it
```


### Config File Walkthroughs
Key Engineering Decisions in this file:
Dual-Source Orchestration:

Unlike most other configs, `pipelines/silver/configs/forest_inventory_annual.yml` lists two sources: the raw FAO metrics and the item code definitions. Joining them at load time ensures the Silver layer contains human-readable terms like "Forest land" instead of numeric codes.
The orchestrator loads both sources into a dictionary and passes them to `process_forest_inventory`. The join logic stays in Python while the input table names stay in YAML.

Granular merge_keys:
Because one country will have multiple rows for different land types (e.g., "Forest land", "Cropland") for the same year, we include item in the merge keys. This ensures the Delta MERGE operation is precise and doesn't accidentally overwrite "Forest" data with "Crop" data.

Terminology Alignment:
The config uses `country_name` in the merge keys to match the output of `process_forest_inventory`. Consistency between YAML and Python is what lets a reader follow the data lineage.

Unit Standardisation Placeholder:
The config declares `unit_standard: "ha"` in the parameters. This signals that the Silver layer guarantees all nature data is in hectares, so it joins directly with other nature datasets in the Gold layer.



`pipelines/silver/configs/energy_metrics.yml` table is particularly important because it provides the socio-economic context (GDP and Population) that the AI/BI Genie will use to normalise climate impacts (e.g., "Energy demand per capita").
Key Engineering Decisions in this file:
Composite Primary Key (merge_keys):
Energy data is annual. By using iso_code and year, we ensure that the Silver layer always has exactly one row per country, per year. This makes joining with the Forestry data later very simple.

Contextual Description:
The description specifically mentions "GDP" and "Population", telling an analyst that no separate economy table is needed: both are already integrated here.

Parameterised Filtering:
The `start_year: 2010` parameter controls the temporal scope of the project from the config file. Extending the project back to the year 2000 is a one-number change; the Python logic stays untouched.

ISO-3166 Standardisation:
The use of iso_code as a merge key enforces a global standard. This ensures that "France" in the Energy table always matches "FRA" in the Weather table.



`pipelines/silver/configs/weather_historical.yml`
Logical Grouping: The config separates Target, Sources, Logic, State, and Params, so it is easy to see where the infrastructure ends and the science begins.

Clear Terminology:
The keys are named `merge_keys`, which tells a developer that this table uses upsert logic rather than simple appends. `watermark_column` signals that this table supports incremental loading.

Genie Hooks: The description field is phrased as a full sentence. This is best practice for the AI/BI Genie, which uses these descriptions to decide which table to use for natural language queries.








`pipelines/silver/configs/dim_locations.yml` defines the Master Location Lookup. It is one of the most critical files in the repository because it creates the "Common Denominator" (ISO Alpha-3 codes) that allows the weather data to be joined with the energy and nature data.

Key Engineering Decisions in this file:
Enforcing the "Single Source of Truth":
By joining FAO and OWID in this specific YAML, we resolve naming discrepancies (e.g., "Czech Republic" vs "Czechia") at the Silver gate. Every downstream table will now use the iso_code from this master list.

The "Common" Module:
This table is assigned to the `common` module because it is shared by the Weather, Energy, and Nature pipelines.

Strict merge_keys:
Only `iso_code` is used as the merge key, so the dimension never contains duplicate entries for the same country, even if the source data is updated multiple times.

Genie Support:
The description explicitly mentions "ISO-3166 Alpha-3." This allows the AI/BI Genie to understand that this table is the "Map" it should use when a user asks a question about a specific region or country.


The best practice is to create `pipelines/silver/configs/dim_date.yml` once in Silver and reference it in Gold.
Why Silver? The Silver layer is the "Standardisation" gate. By creating the dim_date here, you ensure that every standardisation script (like your Weather script) has a "spine" to join against immediately.

Why not repeat it in Gold? Repeating the code in Gold violates the "Don't Repeat Yourself" principle: adding a column such as EU public holidays would later require updating two scripts. Building the calendar in Silver means the Gold layer automatically sees the new columns.

Recommendation: Keep the YAML and the logic in the Silver pipeline. The Gold layer treats `dim_date` as a read-only source.
sources: {}: This is a clear signal to an auditor that this table is "System Generated." It prevents them from wasting time looking for a bronze.date table that doesn't exist.

Seasonality Logic: By including "EU Seasonality" in the description, you tell the AI/BI Genie that it can answer questions like "Show me energy demand for all Winters since 2020."

Extended Range: By extending the calendar to 2040, you ensure the Gold layer can handle the Climate Projections data without the joins failing for future dates.





`pipelines/silver/configs/dim_stations.yml` configuration extracts and standardises the metadata for the NOAA weather stations. It is a critical "bridge" table that ensures every weather station is correctly mapped to a specific Country, allowing for seamless joining between station-level ground truths and national-level energy data.

Key Engineering Decisions in this file:
Deduplication at the Source:
In Bronze, station metadata repeats across millions of daily observation rows. This config triggers a distinct extraction, creating a lean lookup table and removing that redundancy from Silver.

Spatial Anchoring:
The station dimension is deduplicated from raw NOAA observations, creating a lean lookup table. Each station is mapped to its country for join compatibility with weather and energy data.

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


`pipelines/silver/silver_orchestrator.py` uses Delta MERGE logic to ensure Bronze uploads never create duplicate records in Silver. It is the engine that reads the metadata and executes the transformations.
Key Engineering Principles in the Orchestrator:
Dynamic Import (importlib):
The engine has no built-in knowledge of "Weather" or "Energy". It reads the module and function names from the YAML and loads the code dynamically. A new domain is added by creating a new Python file in `transforms/`.

SQL-Based Delta Merge:
On the Free Edition, the Python DeltaTable API can be slow. The orchestrator builds a MERGE statement dynamically from the merge keys in the config, which is the most efficient way to prevent duplicate rows.

State Resilience:
The loop wraps each table in a try/except block. If one table fails (for example, NOAA weather data with a schema error), the orchestrator logs the failure and continues with the remaining tables, so a single run completes as much work as possible.

Genie Integration:
The orchestrator applies the description from the YAML as a `COMMENT ON TABLE`, so AI/BI Genie always has the metadata it needs to answer user questions.

Summary of what this achieves:
* Decoupled logic: the engine is separate from the domain maths.
* Metadata-driven: all table behaviour is controlled via the 10 YAML config files.
* Incremental: only new data is processed, respecting the Free Edition's limits.








## 7: Dual Pipeline Approaches (Notebook Orchestration and SDP Coexistence)
*   **Context:** The Silver and Gold layers need a reliable, maintainable orchestration strategy on Databricks Free Edition. Two viable approaches exist: notebook-based orchestration (config-driven loop) and Lakeflow Spark Declarative Pipelines (SDP, declarative table/MV definitions).
*   **Decision:** Support **both approaches as coexisting options**. The notebook orchestrators (`silver_orchestrator`, `gold_orchestrator`) remain unchanged. SDP source files (`silver_sdp.py`, `gold_sdp.py`) are added alongside them. Both share `src/transforms/*.py`.
*   **Rationale:** Each approach has distinct strengths (see [Pipeline Approaches](../../../pipeline_approaches.md) for the full comparison):
    *   **Notebook orchestration** provides independent per-layer scheduling, failure isolation, and a custom `pipeline_runs` monitoring table.
    *   **SDP** provides automatic checkpointing, built-in data quality expectations, automatic dependency resolution, and an event log.
*   **Coexistence Strategy:** During the coexistence period, each approach writes to separate schemas (`silver`/`gold` for notebooks, `silver_sdp`/`gold_sdp` for SDP). Both read from the same Bronze tables (read-only, no conflict). The DABs manifest (`databricks.yml`) defines both `resources.jobs` (existing) and `resources.pipelines` (new); they are independent and do not interfere.
*   **Pros:**
    *   Engineers can choose the approach that best fits the operational need.
    *   No existing code is disturbed: the notebook orchestrators, configs, and monitoring continue to work as-is.
    *   Both approaches share the same transform functions, so bug fixes benefit both simultaneously.
*   **Cons:**
    *   Two sets of pipeline code to maintain (orchestrator notebooks + SDP source files).
    *   Two monitoring systems (`pipeline_runs` table + SDP event log); a dashboard may need to query both.
    *   Free Edition's one-active-pipeline-per-type limit means SDP Silver and Gold must be combined into a single pipeline.
*   **Alternative:** Convert entirely to SDP and retire the notebook orchestrators. *Rejected for now:* loses per-layer failure isolation and custom monitoring. The dual-approach period allows evaluation before committing.
*   **Standard Practice:** Yes. This follows the Databricks guidance of evaluating declarative pipelines alongside existing job-based orchestration before full migration.

## 8: Modelled vs Observed Grain Mismatch (Known Limitation)
*   **Context:** `fct_ground_truth_verification_daily` compares NOAA GSOD station observations (station grain) against Open-Meteo modelled data (country grain). The modelled data is fetched at a single reference coordinate per country, while the NOAA station is the closest station to that coordinate.
*   **Decision:** Accept the grain mismatch as a known limitation. Most countries have stations near the reference coordinate and show good model-observation agreement (>80% within 2°C tolerance).
*   **Known Exception, Greece:** The single NOAA station (LAMIA) is located in an inland plain (Thessalian Plain) that is significantly hotter than the country-level modelled average. This results in a ~5°C systematic bias and only ~2.5% of records within tolerance for Greece. Other EU countries (France, Germany, Italy, Belgium, Netherlands) achieve 95-100% within tolerance.
*   **Root Cause:** `weather_historical` (Open-Meteo) has no `station_id`; it holds one temperature per `(country, date)`. The gold transform joins on `country + date`, comparing a station's observation against a country-wide model. Greece's diverse topography (mountains, islands, coastal areas vs inland plains) creates large within-country temperature variation.
*   **Future Fix:** Fetch Open-Meteo data at each NOAA station's actual coordinates (requires enriching `dim_stations` with lat/lon from ISD history and creating a station-level modelled table). This would eliminate the grain mismatch for all countries.
*   **Validation:** `TestRealWorldSanity` in `test_gold_tables.py` validates model accuracy per country and documents the Greece exception as a known limitation (see ADR #8). The test issues a warning rather than failing, as the bias is structural and expected with the current architecture.