-- Layer-Specific Setup: Audit Table for Gold Consumption Layer
CREATE TABLE IF NOT EXISTS climate_energy_demand.gold.ingestion_audit (
    table_name STRING,
    last_watermark TIMESTAMP,
    rows_processed INT,
    processed_at TIMESTAMP
)
USING DELTA
COMMENT 'Tracks progress for the Gold consumption layer transformations.';