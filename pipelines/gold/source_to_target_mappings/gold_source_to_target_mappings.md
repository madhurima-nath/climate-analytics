# Gold Layer: Source-to-Target Mappings

Each CSV file documents how columns flow from silver source tables into a single gold target table. One file per gold table. Columns: `Source_Table`, `Source_Column`, `Target_Table`, `Target_Column`, `Logic`.

## Files

| File | Gold Table | What it does |
| --- | --- | --- |
| `energy_climate_sensitivity_silver_gold_mapping.csv` | `fct_energy_demand_daily` | Joins daily weather with national energy baselines. Calculates dynamic HDD/CDD using Koppen base temps, temperature anomaly, heatwave flags, and a demand sensitivity index. |
| `forestry_climate_risk_silver_gold_mapping.csv` | `fct_forest_resilience_annual` | Maps forest carbon health against annual climate stressors at country-year grain. Combines FAO forest inventory with weather aggregates and satellite land cover. |
| `climate_DQ_audit_silver_gold_mapping.csv` | `fct_ground_truth_verification_daily` | Compares physical NOAA station observations against modeled Open-Meteo data. Calculates absolute error, bias, and tolerance flags to validate model accuracy. |
| `fct_temp_change_annual_silver_gold_mapping.csv` | `fct_temp_change_annual` | Promotes FAO temperature anomalies to gold with ISO code resolution and a boolean warming flag for easy filtering. |
| `fct_land_cover_annual_silver_gold_mapping.csv` | `fct_land_cover_annual` | Promotes FAO land cover data to gold with ISO code resolution and area conversion from 1000 ha to ha. |
| `dim_locations_silver_gold_mapping.csv` | `dim_locations` | Enriches country lookup with Koppen climate zone, descriptions, dynamic base temperatures, and a data quality flag from per-station classification. |
| `dim_stations_silver_gold_mapping.csv` | `dim_stations` | Enriches station lookup with Koppen climate zone, description, and a data quality flag from per-station weather observations. |
| `dim_date_silver_gold_mapping.csv` | `dim_date` | Adds heating season (Oct-Mar) and cooling season (Jun-Sep) boolean flags to the silver date dimension. |
| `dim_koppen_zones_silver_gold_mapping.csv` | `dim_koppen_zones` | Static reference table generated from Python dicts. Lists Koppen zone codes, descriptions, and climate-zone-specific degree-day base temperatures. |