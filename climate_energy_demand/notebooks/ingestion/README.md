# Ingestion
This folder pulls raw data from four sources into Databricks. All data lands in the
`climate_energy_demand` catalog, `bronze` schema. Nothing is cleaned or transformed here.
That happens later, in the silver layer.
## Files
**01_ingest_openmeteo_historical.py**
Pulls daily temperature data (2010-2025) for the 5 countries from the Open-Meteo weather API.
This is a one-time backfill. It is safe to run again if needed. It only overwrites rows in its
own date range, so it will not delete newer data added by the incremental notebook below.

Both `01` and `02` write to `climate_energy_demand.bronze.openmeteo_historical`:

| Column | Type | Notes |
|---|---|---|
| country | string | |
| date | string | not a native date type — cast to DATE in silver |
| temperature_2m_max | double | °C |
| temperature_2m_min | double | °C |
| latitude | double | |
| longitude | double | |
| ingested_at | string | used for dedup ordering in silver |

No true daily mean is available from this endpoint — only max/min. The mean is
approximated in silver as `(max + min) / 2`.

**02_ingest_openmeteo_incremental.py**
Adds new days of temperature data after the backfill has run once. It checks the latest date
already saved for each country, then fetches from there forward. It also re-fetches the last
7 days each time, because weather data can be corrected a few days after it is first published.
New and corrected rows are merged in. Nothing else is touched.
**03_ingest_owid_energy.py**
Reads electricity demand data for the 5 countries from Our World in Data. This data does not
come from a live web request. Databricks blocks outbound requests to the domains this file
is hosted on, so the CSV has to be downloaded by hand and uploaded to a Volume first. See the
note in the notebook for the upload steps. This has to be repeated whenever the data needs
refreshing, since OWID updates it about once a year.

Writes to `climate_energy_demand.bronze.owid_energy`, already trimmed at ingestion to the
columns relevant to this project (not OWID's full column set):

| Column | Type | Notes |
|---|---|---|
| country | string | |
| year | bigint | |
| iso_code | string | |
| population | double | |
| electricity_demand | double | |
| electricity_generation | double | |
| primary_energy_consumption | double | |

**04_ingest_noaa_gsod.py**
Pulls daily temperature data from NOAA weather stations, one station per country. This data is
used to check the Open-Meteo data against real station readings. The station for each country
is not hardcoded. The notebook looks up NOAA's full station list and picks the closest station
to each country's reference point automatically. NOAA stopped updating this dataset on
2025-08-29, so this is a one-time pull. There is no incremental version, because there will
never be new data to add.

Writes to `climate_energy_demand.bronze.noaa_gsod`:

| Column | Type | Notes |
|---|---|---|
| country | string | |
| station_id | string | |
| station_name | string | |
| DATE | string | not a native date type |
| TEMP | double | °F — converted to Celsius in silver |
| MAX | double | °F |
| MIN | double | °F |
| PRCP | double | inches — converted to mm in silver |

**05_ingest_openmeteo_cmip6_projections.py**
Pulls CMIP6 climate projection data for the 5 countries from Open-Meteo's Climate API — a
separate product from the Historical Weather API above, with its own endpoint and no shared
parameters. It has no explicit SSP2-4.5 / SSP5-8.5 scenario selector; instead it returns 7
downscaled CMIP6 models, and their spread stands in for scenario uncertainty. Coverage is
1950-2050, not to 2100. Rather than pulling the full range continuously, the notebook samples
7 whole representative years (2020, 2025, 2030, 2035, 2040, 2045, 2050), each pulled complete
(Jan-Dec), to stay well within the free-tier rate limit. This is a one-time pull: the cell can
be run all at once for all 7 years, or split up over multiple sessions by trimming
`SAMPLE_YEARS` down and re-running later — each year writes independently, so nothing already
saved is lost either way. Unlike `01`, this data does include a true daily mean temperature
column, not just max/min.

Writes to `climate_energy_demand.bronze.openmeteo_cmip6_projections`:

| Column | Type | Notes |
|---|---|---|
| country | string | |
| date | string | not a native date type |
| model | string | one of 7 CMIP6 HighResMIP models |
| temperature_2m_mean | double | °C — true mean, no approximation needed |
| temperature_2m_max | double | °C |
| temperature_2m_min | double | °C |
| latitude | double | |
| longitude | double | |
| ingested_at | string | |

Grain is `(country, date, model)`, not `(country, date)` — the model dimension exists
as rows, not columns. Verified complete: 2,557 days/country/model, correct leap-year
count, all 5 countries × 7 models present.

## Why some notebooks are one-time and some are ongoing
- Open-Meteo (historical) updates daily, so it gets both a backfill and an incremental notebook.
- Open-Meteo (climate/CMIP6) projections don't change day to day the way weather does, so a
  one-time sampled pull is sufficient. No incremental version needed.
- OWID updates about once a year, so a full re-download each time is fine. No incremental
  version needed.
- NOAA GSOD will never update again. One-time only.
## A note on the OWID workaround
Databricks Free Edition restricts which external websites it can connect to. Two different
domains hosting the same OWID file both failed. The fix was to download the file manually and
upload it to a Unity Catalog Volume, then read it from there instead of over the internet.
If NOAA or Open-Meteo ever stop working the same way, the same fix would apply: download by
hand, upload to a Volume, read from that path.
