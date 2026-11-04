# Location: src/tests/test_lane_parity.py
# Purpose: Exit criterion 1 (output parity) evidence. Compares the content of
# every table pair between the notebook lane (silver/gold) and the SDP lane
# (silver_sdp/gold_sdp) on business columns, using symmetric EXCEPT ALL.
# Audit metadata written solely by the notebook orchestrator is excluded and
# documented in AUDIT_COLUMNS.

import pytest

# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

CATALOG = "climate_energy_demand"

SILVER_TABLES = [
    "energy_metrics",
    "weather_observations",
    "weather_projections",
    "weather_historical",
    "dim_stations",
    "dim_date",
    "dim_locations",
    "forest_inventory_annual",
    "land_cover_annual",
    "temp_change_annual",
]

GOLD_TABLES = [
    "dim_date",
    "dim_stations",
    "dim_locations",
    "dim_koppen_zones",
    "fct_energy_demand_daily",
    "fct_temp_change_annual",
    "fct_land_cover_annual",
    "fct_forest_resilience_annual",
    "fct_ground_truth_verification_daily",
]

TABLE_PAIRS = (
    [(f"{CATALOG}.silver.{t}", f"{CATALOG}.silver_sdp.{t}") for t in SILVER_TABLES]
    + [(f"{CATALOG}.gold.{t}", f"{CATALOG}.gold_sdp.{t}") for t in GOLD_TABLES]
)

# Columns written only by the notebook orchestrator's MERGE path; the SDP lane
# does not (and should not) carry them. They are pipeline metadata, not data.
AUDIT_COLUMNS = {"_loaded_at", "_run_id", "_updated_at"}


# =============================================================================
# COMPARISON LOGIC
# =============================================================================

def compare_pair(spark, notebook_table, sdp_table):
    """Compare one table pair on business columns.

    Symmetric EXCEPT ALL (multiset semantics): any row present in one lane and
    not the other, including duplicated rows, is counted as a difference.
    Raises AssertionError on any schema drift, row count, or content difference.
    Returns a summary dict on success.
    """
    nb = spark.table(notebook_table)
    sdp = spark.table(sdp_table)

    nb_cols = {f.name: f.dataType.simpleString() for f in nb.schema.fields}
    sdp_cols = {f.name: f.dataType.simpleString() for f in sdp.schema.fields}

    unexpected = ((set(nb_cols) | set(sdp_cols)) - (set(nb_cols) & set(sdp_cols))) - AUDIT_COLUMNS
    assert not unexpected, f"Unexpected column difference beyond audit columns: {sorted(unexpected)}"

    type_diff = {c: (nb_cols[c], sdp_cols[c]) for c in set(nb_cols) & set(sdp_cols) if nb_cols[c] != sdp_cols[c]}
    assert not type_diff, f"Type mismatches on shared columns: {type_diff}"

    business = sorted(set(nb_cols) & set(sdp_cols) - AUDIT_COLUMNS)
    nb_b = nb.select(business)
    sdp_b = sdp.select(business)

    nb_count = nb_b.count()
    sdp_count = sdp_b.count()
    assert nb_count == sdp_count, f"Row counts differ: notebook {nb_count}, SDP {sdp_count}"

    nb_only = nb_b.exceptAll(sdp_b).count()
    sdp_only = sdp_b.exceptAll(nb_b).count()
    assert nb_only == 0 and sdp_only == 0, (
        f"Content differs: {nb_only} row(s) only in notebook lane, {sdp_only} row(s) only in SDP lane"
    )

    return {"table": notebook_table, "columns": len(business), "rows": nb_count}


def run_full_comparison(spark):
    """One-off runner: prints one line per table pair.

    Produces the Validation Record entry in pipeline_approaches.md; the pytest
    parameterisation below is the repeatable form of the same check.
    """
    print(f"{'table pair':<58} {'cols':>4} {'rows':>9}  result")
    for notebook_table, sdp_table in TABLE_PAIRS:
        try:
            r = compare_pair(spark, notebook_table, sdp_table)
            print(f"{notebook_table:<58} {r['columns']:>4} {r['rows']:>9}  MATCH")
        except AssertionError as e:
            print(f"{notebook_table:<58} {'':>4} {'':>9}  DIFF: {e}")


# =============================================================================
# TESTS
# =============================================================================

@pytest.mark.parametrize(
    "notebook_table,sdp_table",
    TABLE_PAIRS,
    ids=[f"{nb.split('.')[1]}.{nb.split('.')[2]}" for nb, _ in TABLE_PAIRS],
)
def test_lane_content_parity(spark, notebook_table, sdp_table):
    """Exit criterion 1: table content must be identical across both lanes."""
    compare_pair(spark, notebook_table, sdp_table)
