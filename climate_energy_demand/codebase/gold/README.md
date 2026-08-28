# Gold Layer Architecture: Climate & Energy Intelligence

The Gold layer is the final "Consumption Zone" of the platform. It denormalizes Silver-layer data into high-value, semantic tables optimized for **Physical Climate Risk (PCR)** analysis, **AI/BI Genie** discovery, and executive dashboards.

## 1. Project Structure
The repository follows the configuration-driven pattern established in the Silver layer:

```text
gold/
├── databricks.yml           # Main Asset Bundle configuration
├── design.md        # Unified engineering logic & metric definitions
├── configs/                 # YAML files (One per target Gold table)
│   ├── fct_energy_demand.yml
│   ├── fct_forest_resilience.yml
│   └── fct_ground_truth_audit.yml
├── src/
│   ├── transforms/          # Python transformation library
│   │   ├── energy.py        # Logic for Story 1 (Energy Sensitivity)
│   │   ├── nature.py        # Logic for Story 2 (Forestry Resilience)
│   │   └── quality.py       # Logic for Story 3 (Ground Truth Audit)
│   ├── orchestrator.py      # Engine for YAML parsing & execution
│   └── logger.py            # Audit, logging, and DQ monitoring                
├── sttm/ # Source-to-Target Mapping (CSV)
│   ├── energy_climate_sensitivity_silver_gold_mapping.csv
│   ├── forestry_climate_risk_silver_gold_mapping.csv
│   └── climate_DQ_audit_silver_gold_mapping.csv
└── README.md                # Execution and deployment guide
```

## 2. System Wiring (The Registry)
The `orchestrator.py` dynamically maps Gold tables to their specific transformation modules based on the YAML definitions.

| Target Table | Configuration File | Python Module | Transformation Function |
| :--- | :--- | :--- | :--- |
| `fct_energy_demand` | `fct_energy_demand.yml` | `transforms.energy` | `process_energy_demand` |
| `fct_forest_resilience` | `fct_forest_resilience.yml`| `transforms.nature` | `process_forest_resilience` |
| `fct_ground_truth_audit`| `fct_ground_truth_audit.yml`| `transforms.quality`| `process_fidelity_audit` |
| `dim_h3_spatial` | `dim_h3_spatial.yml` | `transforms.nature` | `create_dim_h3` |

## 3. Engineering Foundations & Observability

### A. Semantic AI-Readiness
Every YAML configuration includes a `comments` attribute for all columns. The Orchestrator automatically pushes these descriptions to Unity Catalog, enabling the **AI/BI Genie** to interpret natural language queries (e.g., *"Which countries have high heating sensitivity?"*) without additional manual documentation.

### B. Spatial Alignment (H3)
Gold tables utilize **Uber H3 Resolution 6** as the universal join key. This ensures that daily weather metrics and annual carbon flux data are perfectly aligned on a high-performance hexagonal grid, bypassing computationally expensive "Point-in-Polygon" operations.

### C. Audit & Transactional Logging
The `logger.py` utility records every transformation cycle into a centralized `gold_audit_log`. Each entry captures:
*   **Pipeline Lineage:** Run IDs, table names, and timestamps.
*   **Data Integrity:** Input vs. Output row counts to identify data loss.
*   **Performance:** Latency per transformation module for compute optimization.

## 4. Production Workflow (Execution)

### Deployment
To deploy the configurations, Python logic, and the automated Gold Job to the Databricks environment:
```bash
databricks bundle deploy
```

### Execution
The Gold layer is executed via the `orchestrator.py`. It can be run as a full suite or as a targeted refresh for a specific analytical story.

1.  **Full Pipeline:** Runs all 3 stories in sequence.
2.  **Targeted Story Refresh:**
    `--parameters '{"table": "fct_energy_demand"}'`

---
*Note: This layer provides the final semantic foundation for the Databricks AI/BI Dashboards and is designed for always-on accessibility in the Databricks Free Edition.*

---