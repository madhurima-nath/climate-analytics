# Genie Semantic Instructions: Silver Layer

> **Note**: This Genie space is for **business data queries** (energy metrics, weather observations, carbon flux, forest inventory). 
> For pipeline monitoring queries (test results, job status, validation outcomes), use the separate monitoring Genie space.

## Domain
European Climate, Energy, and Nature datasets.

## Data Standards
- All timestamps are standardised to UTC.
- Geospatial data must utilise ISO 3166-1 alpha-2 country codes.
- Energy metrics are reported in Megawatts (MW) unless specified otherwise.

## Query Logic & Governance
- Freshness: When queried regarding 'data freshness', always reference the `last_watermark` column within the `climate_energy_demand.silver.ingestion_audit` table.
- Volume: When queried regarding 'data volume', provide the sum of the `rows_processed` column from `climate_energy_demand.silver.ingestion_audit`.
- Geographical Ambiguity: If a query is ambiguous regarding geography, default to 'EU-27' aggregates.
- Quality Assurance: Always cross-reference the `climate_energy_demand.silver.ingestion_audit` table to ensure that requested metrics align with the latest successful pipeline runs.
