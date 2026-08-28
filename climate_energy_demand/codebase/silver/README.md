# Silver Layer Architecture

This layer standardizes raw Bronze data using **Databricks Asset Bundles (DABs)**. We use a modular approach where each table is defined by a YAML configuration file and processed by a shared library of Python functions.

## 1. Project Structure

*   **`configs/`**: Contains one YAML file per table (e.g., `weather_historical.yml`). These define the source data and the specific transformations to apply.
*   **`lib/transforms.py`**: A library of reusable Python functions (e.g., `calculate_degree_days`, `apply_forward_fill`).
*   **`pipeline_orchestrator.py`**: The Spark engine that reads the YAMLs and runs the transformations.

## 2. Table Catalog

| Domain | Table Name | YAML Config | Description |
| :--- | :--- | :--- | :--- |
| **Shared** | `dim_locations` | `dim_locations.yml` | Master ISO country codes and names |
| **Shared** | `dim_date` | `dim_date.yml` | Calendar with holidays and weekends |
| **Weather** | `weather_historical` | `weather_historical.yml` | Daily temperatures + Degree Day metrics |
| **Weather** | `weather_projections`| `weather_projections.yml` | Future CMIP6 climate scenarios |
| **Weather** | `weather_observations`| `weather_observations.yml`| NOAA sensor data (Metric units) |
| **Weather** | `dim_stations` | `dim_stations.yml` | Metadata for NOAA weather stations |
| **Energy** | `energy_metrics` | `energy_metrics.yml` | Annual demand, population, and GDP |
| **Forestry** | `forest_carbon_annual`| `forest_carbon.yml` | FAO forest area and carbon stocks |
| **Forestry** | `carbon_flux_spatial`| `carbon_flux.yml` | GFW Carbon Sink/Source by H3 grid |
| **Spatial** | `dim_h3_grid` | `dim_h3_grid.yml` | Land-use classification (H3 Res 6) |

## 3. Deployment
The entire layer is managed as a single bundle:
```bash
databricks bundle deploy