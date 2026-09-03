# Climate, Energy & Nature: Silver Layer

This repository contains the engineering logic for the **Silver Layer** of a multi-domain data platform. The system harmonises disparate datasets from the FAO, OWID, NOAA, and OpenMeteo into a unified, query-ready state.

## Core Objectives
1. **Standardisation:** Converting raw API responses and manual uploads into consistent, metric-standardised tables.
2. **Geospatial Alignment:** Bridging the gap between coordinate-based weather data and area-based forestry metrics.
3. **Temporal Continuity:** Creating a daily time-series from annual and daily sources.

## Repository Structure
*   `setup/`: **Bootstrap Scripts.** SQL and Python scripts to create schemas, the audit table, and the H3 land-mask reference.
*   `common/`: **Logic Library.** Pure Python functions for Relational Normalisation, Geospatial Indexing, and Audit Management.
*   `silver/`: **Workflow Orchestrators.** Domain-specific pipelines (FAO, Weather, Energy) that apply library logic to Bronze sources.
*   `docs/`: **Technical Documentation.** Detailed design logic and architectural decision records.

## Deployment
The project is managed as a **Databricks Asset Bundle (DAB)**. To deploy the pipelines to the Databricks Community Edition, follow the instructions in the `/deployment` folder.