# Ingestion

This folder pulls raw data from three sources into Databricks. All data lands in the
`climate_energy_demand` catalog, `bronze` schema. Nothing is cleaned or transformed here.
That happens later, in the silver layer.

## Files

**01_ingest_openmeteo_historical.py**
Pulls daily temperature data (2010-2025) for the 5 countries from the Open-Meteo weather API.
This is a one-time backfill. It is safe to run again if needed. It only overwrites rows in its
own date range, so it will not delete newer data added by the incremental notebook below.

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

**04_ingest_noaa_gsod.py**
Pulls daily temperature data from NOAA weather stations, one station per country. This data is
used to check the Open-Meteo data against real station readings. The station for each country
is not hardcoded. The notebook looks up NOAA's full station list and picks the closest station
to each country's reference point automatically. NOAA stopped updating this dataset on
2025-08-29, so this is a one-time pull. There is no incremental version, because there will
never be new data to add.

## Why some notebooks are one-time and some are ongoing

- Open-Meteo updates daily, so it gets both a backfill and an incremental notebook.
- OWID updates about once a year, so a full re-download each time is fine. No incremental
  version needed.
- NOAA GSOD will never update again. One-time only.

## A note on the OWID workaround

Databricks Free Edition restricts which external websites it can connect to. Two different
domains hosting the same OWID file both failed. The fix was to download the file manually and
upload it to a Unity Catalog Volume, then read it from there instead of over the internet.
If NOAA or Open-Meteo ever stop working the same way, the same fix would apply: download by
hand, upload to a Volume, read from that path.
