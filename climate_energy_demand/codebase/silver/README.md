# Silver Layer Architecture: Unified Climate & Energy Foundation

The Silver layer transforms raw Bronze data into a standardized, relational format. It is implemented as a **Databricks Asset Bundle (DAB)**, separating transformation logic (Python) from table configuration (YAML).

## 1. Project Structure
The repository is organized to ensure modularity and scalability:

```text
silver/
├── databricks.yml           # Main Asset Bundle configuration
├── configs/                 # 10 YAML files (one per target table)
├── src/
│   ├── transforms/          # Python transformation library
│   │   ├── common.py        # Shared dimensions
│   │   ├── energy.py        # Energy metrics
│   │   ├── geospatial.py    # H3 and Spatial logic
│   │   ├── nature.py        # Forest and Carbon logic
│   │   └── weather.py       # Weather and Station logic
│   └── orchestrator.py      # The engine that executes the YAMLs
├── README.md                # Deployment and Execution guide
└── DESIGN.md                # Engineering logic and reasoning
```

## 2. System Wiring (The Registry)
The `orchestrator.py` engine uses the configuration files to dynamically map datasets to their specific transformation functions.

| Target Table | Configuration File | Python Module | Transformation Function |
| :--- | :--- | :--- | :--- |
| `dim_locations` | `dim_locations.yml` | `common` | `create_dim_locations` |
| `dim_date` | `dim_date.yml` | `common` | `create_dim_date` |
| `dim_stations` | `dim_stations.yml` | `weather` | `create_dim_stations` |
| `dim_h3_grid` | `dim_h3_grid.yml` | `geospatial` | `create_dim_h3_grid` |
| `weather_historical` | `weather_historical.yml` | `weather` | `process_weather_historical` |
| `weather_observations` | `weather_observations.yml`| `weather` | `process_weather_observations` |
| `weather_projections` | `weather_projections.yml` | `weather` | `process_weather_projections` |
| `energy_metrics` | `energy_metrics.yml` | `energy` | `process_energy_metrics` |
| `forest_carbon_annual` | `forest_carbon_annual.yml` | `nature` | `process_forest_carbon_annual` |
| `carbon_flux_spatial` | `carbon_flux_spatial.yml` | `geospatial` | `process_carbon_flux_spatial` |


## 3. Production Workflow (Execution)

### Deployment
To deploy the code, configurations, and the automated Job to Databricks:
```bash
databricks bundle deploy
```

### Summary of the "Wiring":
*   **`databricks.yml`**: Defines a **Job** called `silver_pipeline_job`.
*   **The Job**: Tasked with running `src/orchestrator.py`.
*   **`orchestrator.py`**: Takes an optional parameter `table`. If provided, it runs that one YAML. If not, it loops through the whole `configs/` folder.

### Execution
Once deployed, the Silver layer is executed via the **Orchestrator Job**. This move away from manual notebook execution ensures that the pipeline is reproducible and can be scheduled.

1. **Full Pipeline:** By default, the job processes all 10 tables in the correct order.
2. **Targeted Refresh:** Developers can refresh a single table by passing a parameter to the job:
   `--parameters '{"table": "weather_historical"}'`
   This prevents unnecessary re-processing of the entire global dataset when only one domain needs updating.
```

## 4. System Wiring & Traceability

This project utilizes a **Dynamic Mapping Pattern** to connect configurations to logic. This approach ensures that the pipeline orchestrator remains generic, while the business logic is isolated in domain-specific modules.

### The Configuration-to-Logic Mapping
Every table in the Silver layer follows a strict "Wiring" contract. The `orchestrator.py` parses the YAML and uses Python's `importlib` to dynamically load the required module and function. 

| Target Table | Config YAML | Python Module | Transformation Function |
| :--- | :--- | :--- | :--- |
| `dim_locations` | `dim_locations.yml` | `transforms/common.py` | `create_dim_locations` |
| `dim_date` | `dim_date.yml` | `transforms/common.py` | `create_dim_date` |
| `dim_stations` | `dim_stations.yml` | `transforms/weather.py` | `create_dim_stations` |
| `dim_h3_grid` | `dim_h3_grid.yml` | `transforms/geospatial.py` | `create_dim_h3_grid` |
| `weather_historical` | `weather_historical.yml` | `transforms/weather.py` | `process_weather_historical` |
| `weather_observations` | `weather_observations.yml`| `transforms/weather.py` | `process_weather_observations` |
| `weather_projections` | `weather_projections.yml` | `transforms/weather.py` | `process_weather_projections` |
| `energy_metrics` | `energy_metrics.yml` | `transforms/energy.py` | `process_energy_metrics` |
| `forest_carbon_annual` | `forest_carbon_annual.yml` | `transforms/nature.py` | `process_forest_carbon_annual` |
| `carbon_flux_spatial` | `carbon_flux_spatial.yml` | `transforms/geospatial.py` | `process_carbon_flux_spatial` |

### Why this matters (Engineering Reasoning)
1. **Decoupled Architecture:** The orchestrator doesn't need to be updated when a new table is added. It simply looks for a new YAML and the corresponding function.
2. **Transparent Lineage:** By naming the YAML files after the target tables, we provide a clear "paper trail" for anyone auditing the pipeline.
3. **Domain Isolation:** If a change is required for **Climate Logic** (e.g., adjusting the 15°C HDD threshold), the developer only needs to touch `weather.py` and `weather_historical.yml`, with zero risk of impacting the **Forestry** or **Energy** codebases.