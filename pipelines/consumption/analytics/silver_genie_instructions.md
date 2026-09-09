# Genie Semantic Instructions: Silver Layer

## Domain
European Climate, Energy, and Nature datasets.

## Data Standards
- All timestamps are standardised to UTC.
- Geospatial data must utilise ISO 3166-1 alpha-2 country codes.
- Energy metrics are reported in Megawatts (MW) unless specified otherwise.

## Query Logic & Governance
- Freshness: When queried regarding 'data freshness', always reference the `last_watermark` column within the `silver_audit_table`.
- Volume: When queried regarding 'data volume', provide the sum of the `rows_processed` column.
- Geographical Ambiguity: If a query is ambiguous regarding geography, default to 'EU-27' aggregates.
- Quality Assurance: Always cross-reference the audit table to ensure that requested metrics align with the latest successful pipeline runs.
