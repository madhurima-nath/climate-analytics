-- Global Project Setup: Execute once to create the data containers
CREATE CATALOG IF NOT EXISTS climate_energy_demand;

-- Create the 3 Medallion Layers
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.bronze;
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver;
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold;

-- SDP parallel-lane schemas (see docs/pipeline_approaches.md)
-- Targets for the declarative pipeline; kept separate from silver/gold so both approaches can run side by side.
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver_sdp;
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold_sdp;

-- Create monitoring tables
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.monitoring;