# Genie Semantic Instructions: Silver Layer

> **Note**: This Genie space is for **business data queries** (energy metrics, weather observations, land cover, forest inventory, temperature change). 
> For pipeline monitoring queries (test results, job status, validation outcomes), use the separate monitoring Genie space.

## Domain
Global Climate, Energy, and Nature datasets covering 250+ territories.

## Data Standards
- All timestamps are standardised to UTC.
- Geospatial data must utilise ISO 3166-1 alpha-3 country codes.
- Energy metrics are reported in Megawatts (MW) unless specified otherwise.

## Query Logic & Governance
- Freshness: When queried regarding 'data freshness', always reference the `last_watermark` column within the `climate_energy_demand.silver.ingestion_audit` table.
- Volume: When queried regarding 'data volume', provide the sum of the `rows_processed` column from `climate_energy_demand.silver.ingestion_audit`.
- Geographical Ambiguity: If a query is ambiguous regarding geography, default to global aggregates or request clarification.
- Quality Assurance: Always cross-reference the `climate_energy_demand.silver.ingestion_audit` table to ensure that requested metrics align with the latest successful pipeline runs.
