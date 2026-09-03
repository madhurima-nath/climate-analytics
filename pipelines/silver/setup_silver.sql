-- Layer-Specific Setup: Audit Table for Silver Standardisation
CREATE TABLE IF NOT EXISTS climate_energy_demand.silver.ingestion_audit (
    table_name STRING,
    last_watermark TIMESTAMP,
    rows_processed INT,
    processed_at TIMESTAMP
) USING DELTA;

COMMENT ON TABLE climate_energy_demand.silver.ingestion_audit IS 'Tracks progress for the Silver standardisation layer.';