-- Global Project Setup: Execute once to create the data containers
CREATE CATALOG IF NOT EXISTS climate_energy_demand;

-- Create the 3 Medallion Layers
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.bronze;
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.silver;
CREATE SCHEMA IF NOT EXISTS climate_energy_demand.gold;