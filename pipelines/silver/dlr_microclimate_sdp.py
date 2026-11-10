# =============================================================================
# Silver Layer: DLR Micro-Climate Feature (SDP Source) - Step 1
# =============================================================================
# Synthetic micro-climate vectors, Method B country trust constants, and the
# climate stress-test scenario horizons for the dynamic line rating (DLR)
# feature. SDP-native scope: see dlr_design.md at the bundle root.
# This file is isolated from silver_sdp.py / gold_sdp.py; no existing dataset
# is modified. All datasets are materialized views in silver_sdp.
#
# Datasets:
#   1. micro_climate_synthetic_vectors    - deterministic mock physics
#      (wind, solar, congestion penalty), country-day, 2010-2040, IEEE 738
#      bounds. Hash-derived noise only; no rand(); stable across refreshes.
#   2. dim_country_historical_trust_metadata - Method B country constants.
#      Base ratings are declared engineering constants (dlr_design.md 4.1);
#      MAE, mean temperature and the vulnerability coefficient are computed
#      from the 2023-01-01 to 2025-08-24 NOAA verification window.
#   3. weather_historical_anomaly_daily   - helper: the project's custom rule
#      (rolling 10-year monthly mean on UNSHIFTED real history) applied to
#      weather_historical, producing temp_anomaly per country-day.
#   4. climate_stress_scenarios_daily     - 2027-2040 horizons recycling real
#      2011-2024 days re-dated +16 years, warmed 0/+2/+4 C. Scenario anomaly
#      = historical anomaly + shift exactly; no synthetic date overlaps the
#      real observed record.
# =============================================================================

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

_COUNTRIES = ["Germany", "Belgium", "Singapore"]

# Per-country nominal base ratings - declared engineering constants from
# dlr_design.md section 4.1 (Germany/Belgium: 1,000 A continental 400 kV
# baseline; Singapore: 800 A tropical structural de-rating).
_BASE_RATINGS = [("Germany", 1000.0), ("Belgium", 1000.0), ("Singapore", 800.0)]

# Scenario horizons: name -> temperature shift in degrees C
_SCENARIOS = [
    ("baseline", 0.0),
    ("moderate_warming_2c", 2.0),
    ("extreme_heat_dome_4c", 4.0),
]


def _u(channel, country_col, date_col):
    """Deterministic pseudo-random draw in [0, 1) from a hash of the channel,
    country and date. Hash-derived, so values are stable across refreshes.
    rand() is never used."""
    hashed = F.xxhash64(
        F.concat_ws("|", lit(channel), country_col, date_col.cast("string"))
    )
    return F.pmod(hashed, lit(1000000)).cast("double") / lit(1000000.0)


# -----------------------------------------------------------------------------
# 1. micro_climate_synthetic_vectors
#    Pure generator: 2010-01-01 to 2040-12-31 x 3 countries = 33,969 rows
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.micro_climate_synthetic_vectors",
    comment=(
        "Synthetic micro-climate parameters modelling conductor convective "
        "and radiative thermal balancing under IEEE 738 (DLR feature)."
    ),
)
def micro_climate_synthetic_vectors():
    dates = spark.sql(
        "SELECT explode(sequence(to_date('2010-01-01'), "
        "to_date('2040-12-31'), interval 1 day)) AS date"
    )
    countries = spark.createDataFrame([(c,) for c in _COUNTRIES], "country STRING")

    df = (
        dates.crossJoin(countries)
        .withColumn("doy", F.dayofyear(col("date")))
        .withColumn("m", F.month(col("date")))
        .withColumn("u_wind", _u("wind", col("country"), col("date")))
        .withColumn("u_solar", _u("solar", col("country"), col("date")))
        .withColumn("u_pen", _u("penalty", col("country"), col("date")))
    )

    two_pi = lit(2.0 * 3.141592653589793)

    # Wind speed (m/s). Germany locked at 0.6 (SLR baseline: no forced
    # convective cooling). Belgium/Singapore dynamic within IEEE 738 bounds.
    wind = (
        F.when(col("country") == "Germany", lit(0.6))
        .when(
            col("country") == "Belgium",
            # Winter-storm-peaked seasonal curve + noise, clamped [0.6, 8.0]
            F.greatest(
                lit(0.6),
                F.least(
                    lit(8.0),
                    lit(3.0)
                    + lit(1.5)
                    * (-F.cos(two_pi * (col("doy") - 15) / lit(365.0)))
                    + lit(3.5) * col("u_wind"),
                ),
            ),
        )
        .otherwise(
            # Singapore: monsoon-modulated + noise, clamped [0.6, 8.0]
            F.greatest(
                lit(0.6),
                F.least(
                    lit(8.0),
                    lit(3.2)
                    + lit(0.9) * F.cos(two_pi * (col("doy") - 200) / lit(365.0))
                    + lit(3.0) * col("u_wind"),
                ),
            )
        )
    )

    # Solar radiation (W/m2). Strong summer seasonality for Germany/Belgium;
    # high and flat for Singapore. Top-decile summer days get a 1.15 heatwave
    # boost. Clamped to [100, 900].
    solar_base = (
        F.when(
            col("country") == "Singapore",
            lit(620.0)
            + lit(90.0) * F.sin(two_pi * (col("doy") - 81) / lit(365.0))
            + lit(120.0) * (lit(2.0) * col("u_solar") - lit(1.0)),
        )
        .otherwise(
            lit(420.0)
            + lit(340.0) * F.sin(two_pi * (col("doy") - 81) / lit(365.0))
            + lit(120.0) * (lit(2.0) * col("u_solar") - lit(1.0)),
        )
    )
    in_summer = (
        F.when(col("country") == "Singapore", col("m").between(3, 9))
        .otherwise(col("m").between(6, 8))
    )
    heatwave_boost = (
        F.when(in_summer & (col("u_solar") > lit(0.90)), lit(1.15)).otherwise(lit(1.0))
    )
    solar = F.least(lit(900.0), F.greatest(lit(100.0), solar_base * heatwave_boost))

    # Congestion penalty (EUR per MWh). 40.00 floor x synthesized demand-
    # sensitivity factor: winter-peaked 1.0-2.4 for Germany/Belgium;
    # flat-high 1.2-2.0 for Singapore. No join to OWID is made.
    sensitivity = (
        F.when(
            col("country") == "Singapore",
            lit(1.2)
            + lit(0.3)
            * (
                (lit(1.0) + F.sin(two_pi * (col("doy") - 120) / lit(365.0)))
                / lit(2.0)
            )
            + lit(0.5) * col("u_pen"),
        )
        .otherwise(
            lit(1.0)
            + lit(0.8)
            * ((lit(1.0) + F.cos(two_pi * (col("doy") - 15) / lit(365.0))) / lit(2.0))
            + lit(0.6) * col("u_pen"),
        )
    )
    penalty = lit(40.00) * sensitivity

    return df.select(
        col("country"),
        col("date"),
        F.round(wind, 2).alias("simulated_wind_speed_ms"),
        F.round(solar, 1).alias("simulated_solar_radiation_wm2"),
        F.round(penalty, 2).alias("simulated_congestion_penalty_eur"),
    )


# -----------------------------------------------------------------------------
# 2. dim_country_historical_trust_metadata
#    Method B (dlr_design.md section 7): country constants computed from the
#    2023-2025 NOAA verification window. Never hardcoded.
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.dim_country_historical_trust_metadata",
    comment=(
        "Method B country trust constants: nominal base ratings (engineering "
        "constants) and computed historical MAE, mean temperature and "
        "vulnerability coefficient from the 2023-2025 NOAA window (DLR feature)."
    ),
)
def dim_country_historical_trust_metadata():
    verification = dlt.read(
        "climate_energy_demand.gold_sdp.fct_ground_truth_verification_daily"
    )
    filtered = verification.filter(
        col("country").isin(_COUNTRIES)
        & (col("date") >= F.to_date(lit("2023-01-01")))
        & (col("date") <= F.to_date(lit("2025-08-24")))
    )
    stats = filtered.groupBy("country").agg(
        F.avg("absolute_error").alias("historical_mae_c"),
        F.avg("observed_avg_temp").alias("historical_mean_temp_c"),
    )
    ratings = spark.createDataFrame(
        _BASE_RATINGS, "country STRING, nominal_base_rating_amps DOUBLE"
    )
    return (
        stats.join(ratings, "country")
        .withColumn(
            "historical_vulnerability_coefficient",
            F.round(col("historical_mae_c") / col("historical_mean_temp_c"), 6),
        )
        .select(
            "country",
            "nominal_base_rating_amps",
            "historical_mae_c",
            "historical_mean_temp_c",
            "historical_vulnerability_coefficient",
        )
    )


# -----------------------------------------------------------------------------
# 3. weather_historical_anomaly_daily (helper)
#    The project's custom rule (src/transforms/energy.py): monthly baseline =
#    rolling 10-year mean (~3650 rows back) partitioned by country + month;
#    anomaly = temp_mean_c - baseline. Computed on the UNSHIFTED real history.
#    Also carries temp_max_c: rating physics (static derating, choke
#    thresholds) reads the daily MAXIMUM ambient temperature; heatwave and
#    anomaly stay on the mean (design decision, Nov 2026 - see dlr_design.md).
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.weather_historical_anomaly_daily",
    comment=(
        "Real weather history with rolling 10-year monthly baseline and "
        "temperature anomaly under the project's custom EU/WMO rule (DLR "
        "feature helper; also feeds the gold DLR fact in Step 2)."
    ),
)
def weather_historical_anomaly_daily():
    src = dlt.read("climate_energy_demand.silver_sdp.weather_historical")
    # DLR feature scope: only the three target grid zones.
    src = src.filter(col("country").isin(_COUNTRIES))
    df = src.select(
        col("country"), col("date").cast("date").alias("date"),
        col("temp_mean_c"), col("temp_max_c"),
    )
    month_window = (
        Window.partitionBy("country", F.month(col("date")))
        .orderBy(col("date"))
        .rowsBetween(-3650, 0)
    )
    return (
        df.withColumn("monthly_baseline_temp", F.avg("temp_mean_c").over(month_window))
        .withColumn("temp_anomaly", col("temp_mean_c") - col("monthly_baseline_temp"))
    )


# -----------------------------------------------------------------------------
# 4. climate_stress_scenarios_daily
#    Recycle real 2011-2024 days re-dated +16 years into 2027-2040, warmed by
#    the scenario shift. Anomaly and heatwave use the UNSHIFTED baseline, so
#    scenario anomaly = historical anomaly + shift exactly (dlr_design.md
#    section 8). No synthetic date overlaps the real observed record.
#    scenario_temp_max_c = historical daily MAX + shift: this is what the
#    gold horizon's choke thresholds read (rating physics on daily max).
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.silver_sdp.climate_stress_scenarios_daily",
    comment=(
        "Climate stress-test horizons 2027-2040: real 2011-2024 days recycled "
        "and warmed 0/+2/+4 C under three scenarios, with anomaly and heatwave "
        "recomputed against the unshifted 10-year baseline (DLR feature)."
    ),
)
def climate_stress_scenarios_daily():
    anomaly = dlt.read(
        "climate_energy_demand.silver_sdp.weather_historical_anomaly_daily"
    )
    hist = anomaly.filter(
        col("country").isin(_COUNTRIES)
        & (F.year(col("date")) >= 2011)
        & (F.year(col("date")) <= 2024)
    )
    scenarios = spark.createDataFrame(
        _SCENARIOS, "scenario STRING, temp_shift_c DOUBLE"
    )
    return (
        hist.crossJoin(scenarios)
        .withColumn("historical_date", col("date"))
        .withColumn("date", F.expr("add_months(date, 192)"))  # +16 years: 2011->2027 ... 2024->2040
        .withColumn("scenario_temp_mean_c", col("temp_mean_c") + col("temp_shift_c"))
        .withColumn("scenario_temp_max_c", col("temp_max_c") + col("temp_shift_c"))
        .withColumn("temp_anomaly", col("temp_anomaly") + col("temp_shift_c"))
        .withColumn("heatwave_flag", (col("temp_anomaly") > lit(5.0)).cast("int"))
        .select(
            "country",
            "scenario",
            "date",
            "historical_date",
            "temp_shift_c",
            "temp_mean_c",
            "temp_max_c",
            "scenario_temp_mean_c",
            "scenario_temp_max_c",
            "monthly_baseline_temp",
            "temp_anomaly",
            "heatwave_flag",
        )
    )
