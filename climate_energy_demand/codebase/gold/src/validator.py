def run_dq_checks(spark, table_name):
    """
    Senior-level DQ Assertions. If these fail, we flag the audit log.
    """
    checks = {
        "fct_energy_demand_daily": [
            "avg_temperature_c BETWEEN -60 AND 60", # Physical possibility
            "demand_sensitivity_index >= 0",        # Logical possibility
            "iso_code RLIKE '^[A-Z]{3}$'"           # ISO-3166 Compliance
        ],
        "fct_forest_resilience_annual": [
            "extreme_heat_days_count <= 366",
            "sequestration_efficiency IS NOT NULL"
        ]
    }
    
    if table_name in checks:
        for condition in checks[table_name]:
            fail_count = spark.table(f"gold.{table_name}").filter(f"NOT ({condition})").count()
            if fail_count > 0:
                raise ValueError(f"DQ Check Failed: {condition} found {fail_count} bad rows.")