# =============================================================================
# Gold Layer: DLR Micro-Climate Feature (SDP Source) - Step 2
# =============================================================================
# The two gold facts of the DLR feature (dlr_design.md sections 5.4-5.5):
#
#   1. fct_dlr_operational_efficiency_daily - country-day, 2010-01-01 to
#      2026-09-20 (real Open-Meteo coverage): trust-gated dynamic line rating
#      vs static rating, unlocked capacity in MW, congestion costs avoided,
#      and the wildfire overlay driven by the forest wildfire resilience
#      score (dlr_design.md section 4.3). A warn-mode SDP expectation asserts
#      heatwave parity against fct_energy_demand_daily over its 2010-2025
#      window (joined via a declared country->ISO3 mapping).
#
#   2. fct_dlr_climate_stress_horizon_annual - country-scenario-year, 2027-2040
#      (126 rows): projected critical throttling days per year on the scenario
#      daily MAXIMUM temperature (choke: 35 + 0.05 x base / 15), plus the UN
#      climate risk score Hazard x Exposure x Vulnerability with the three
#      pillar components materialised for transparency.
#
# Rating physics reads the DAILY MAXIMUM ambient temperature; heatwave
# detection and anomalies stay on the daily mean (decision, 10 Nov 2026 -
# dlr_design.md section 4 parameter registry). Trust gate: live NOAA flag
# where observations exist (|error| <= 2.0C, 2023-01-01 to 2025-08-24), else
# the Method B country constant from dim_country_historical_trust_metadata
# (dlr_design.md section 7) - never a hardcoded date boundary.
# =============================================================================

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

_GOLD = "climate_energy_demand.gold_sdp"
_SILVER = "climate_energy_demand.silver_sdp"
_COUNTRIES = ["Germany", "Belgium", "Singapore"]
_ISOS = ["DEU", "BEL", "SGP"]

# Declared country -> OWID ISO-3 mapping (the three fixed feature countries)
_ISO_MAP = [("Germany", "DEU"), ("Belgium", "BEL"), ("Singapore", "SGP")]


def _forest_resilience_yearly():
    """Per-country-year forest wildfire resilience score (dlr_design.md 4.3).

    R = min(1, carbon_density / own-historical-max density
              x forest_area / 2010 forest_area)
        x exp(-0.4 x Z)
    where Z is the annual mean of positive daily temperature anomalies
    (in degrees C; cool/normal days contribute 0) from the silver helper.
    Grounding: Forzieri et al. 2022 self-referencing health baseline;
    EFFIS/FWI-style climate-relative stress (anomaly vs own normals, not a
    fixed absolute threshold). Returns (country, res_year, resilience_score).
    """
    fao = (
        dlt.read(f"{_GOLD}.fct_forest_resilience_annual")
        .filter(col("iso_code").isin(_ISOS))
        .select(
            col("iso_code"),
            col("year").cast("int").alias("res_year"),
            col("carbon_density"),
            col("forest_area_ha"),
        )
    )
    dens_max = fao.groupBy("iso_code").agg(
        F.max("carbon_density").alias("max_density")
    )
    area_2010 = (
        fao.filter(col("res_year") == 2010)
        .groupBy("iso_code")
        .agg(F.first("forest_area_ha").alias("area_2010"))
    )
    anomaly = dlt.read(f"{_SILVER}.weather_historical_anomaly_daily")
    z = anomaly.groupBy("country", F.year(col("date")).alias("res_year")).agg(
        F.avg(F.greatest(col("temp_anomaly"), lit(0.0))).alias("z_pos_c")
    )
    iso = spark.createDataFrame(_ISO_MAP, "country STRING, iso_code STRING")
    z_iso = z.join(iso, "country").select("iso_code", "res_year", "z_pos_c")

    joined = (
        fao.join(dens_max, "iso_code")
        .join(area_2010, "iso_code")
        .join(z_iso, ["iso_code", "res_year"])
    )
    health = F.least(
        lit(1.0),
        (col("carbon_density") / col("max_density"))
        * (col("forest_area_ha") / col("area_2010")),
    )
    scored = joined.select(
        col("iso_code"),
        col("res_year"),
        (health * F.exp(lit(-0.4) * col("z_pos_c"))).alias("resilience_score"),
    )
    return scored.join(iso, "iso_code").select(
        col("country"), col("res_year"), col("resilience_score")
    )


# -----------------------------------------------------------------------------
# 1. fct_dlr_operational_efficiency_daily
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_dlr_operational_efficiency_daily",
    comment=(
        "Daily DLR operational efficiency: trust-gated dynamic vs static "
        "line ratings, unlocked capacity (MW), congestion costs avoided and "
        "wildfire overlay (resilience score per dlr_design.md 4.3) (DLR feature)."
    ),
)
@dlt.expect("heatwave_parity_ok", "heatwave_parity_ok")
def fct_dlr_operational_efficiency_daily():
    weather = dlt.read(f"{_SILVER}.weather_historical_anomaly_daily")
    mock = dlt.read(f"{_SILVER}.micro_climate_synthetic_vectors")
    trust = dlt.read(f"{_SILVER}.dim_country_historical_trust_metadata")
    verification = dlt.read(f"{_GOLD}.fct_ground_truth_verification_daily")
    energy = dlt.read(f"{_GOLD}.fct_energy_demand_daily").filter(
        col("iso_code").isin(_ISOS)
    )

    # Live NOAA trust: one station per country; average absolute error per
    # country-day guards against any future multi-station expansion.
    live = (
        verification.filter(col("country").isin(_COUNTRIES))
        .groupBy("country", "date")
        .agg(F.avg("absolute_error").alias("live_abs_error_c"))
    )

    # Resilience score per country-day, hold-last-known: the latest FAOSTAT
    # year <= the row's year (FAOSTAT ends 2025; 2026 rows carry the 2025 score).
    res_yearly = _forest_resilience_yearly()
    res_daily = (
        weather.select(
            col("country"), col("date"), F.year(col("date")).alias("yr")
        ).alias("d")
        .join(
            res_yearly.alias("r"),
            (col("d.country") == col("r.country"))
            & (col("r.res_year") <= col("d.yr")),
        )
        .groupBy(col("d.country"), col("d.date"))
        .agg(
            F.max(
                F.struct(col("r.res_year"), col("r.resilience_score"))
            ).alias("last_known")
        )
        .select(
            col("d.country").alias("res_country"),
            col("d.date").alias("res_date"),
            col("last_known.resilience_score").alias("forest_resilience_score"),
        )
    )

    # Heatwave parity reference: the OWID-joined energy fact ends 2025-12-31.
    iso = spark.createDataFrame(_ISO_MAP, "country STRING, iso_code STRING")
    ref = (
        energy.select(
            col("iso_code"),
            col("date"),
            col("is_heatwave_event").cast("int").alias("ref_heatwave"),
        )
        .join(iso, "iso_code")
        .select("country", "date", "ref_heatwave")
    )

    base = (
        weather.alias("w")
        .join(
            mock.alias("m"),
            (col("w.country") == col("m.country"))
            & (col("w.date") == col("m.date")),
        )
        .join(trust.alias("t"), col("w.country") == col("t.country"))
        .join(
            live.alias("l"),
            (col("w.country") == col("l.country"))
            & (col("w.date") == col("l.date")),
            "left",
        )
        .join(
            res_daily.alias("rr"),
            (col("w.country") == col("rr.res_country"))
            & (col("w.date") == col("rr.res_date")),
            "left",
        )
        .join(
            ref.alias("p"),
            (col("w.country") == col("p.country"))
            & (col("w.date") == col("p.date")),
            "left",
        )
    )

    heatwave = (col("w.temp_anomaly") > lit(5.0)).cast("int")
    static = col("t.nominal_base_rating_amps") - lit(15.0) * F.greatest(
        lit(0.0), col("w.temp_max_c") - lit(35.0)
    )
    trusted = (
        F.when(
            col("l.live_abs_error_c").isNotNull(),
            (col("l.live_abs_error_c") <= lit(2.0)).cast("int"),
        ).otherwise((col("t.historical_mae_c") <= lit(2.0)).cast("int"))
    )
    dynamic = (
        F.when(
            trusted == 1,
            F.least(
                lit(1400.0),
                F.greatest(
                    lit(600.0),
                    static
                    + lit(110.0) * col("m.simulated_wind_speed_ms")
                    - lit(0.15) * col("m.simulated_solar_radiation_wm2"),
                ),
            ),
        ).otherwise(static)
    )
    unlocked_mw = (dynamic - static) * lit(0.400) * F.sqrt(lit(3.0))
    wildfire = (
        F.when(
            (heatwave == 1)
            & (col("rr.forest_resilience_score") < lit(0.4)),
            lit("CRITICAL RISK"),
        ).otherwise(lit("NORMAL"))
    )
    parity_ok = (
        F.when(col("p.ref_heatwave").isNull(), lit(True))
        .otherwise(col("p.ref_heatwave") == heatwave)
    )

    return base.select(
        col("w.country").alias("country"),
        col("w.date").alias("date"),
        col("w.temp_max_c").alias("temperature"),
        col("w.temp_mean_c").alias("temperature_mean_c"),
        heatwave.alias("heatwave_flag"),
        trusted.alias("noaa_model_is_trusted"),
        static.alias("static_line_rating_amps"),
        dynamic.alias("dynamic_line_rating_amps"),
        F.round(unlocked_mw, 6).alias("unlocked_capacity_mw"),
        F.round(
            unlocked_mw * col("m.simulated_congestion_penalty_eur"), 6
        ).alias("congestion_costs_avoided_eur"),
        col("rr.forest_resilience_score").alias("forest_resilience_score"),
        wildfire.alias("wildfire_hazard_status"),
        parity_ok.alias("heatwave_parity_ok"),
    )


# -----------------------------------------------------------------------------
# 2. fct_dlr_climate_stress_horizon_annual
# -----------------------------------------------------------------------------

@dlt.table(
    name="climate_energy_demand.gold_sdp.fct_dlr_climate_stress_horizon_annual",
    comment=(
        "Annual climate stress horizon 2027-2040: projected critical "
        "throttling days on scenario daily max temperature and the UN climate "
        "risk score (Hazard x Exposure x Vulnerability) with pillar components "
        "(DLR feature)."
    ),
)
def fct_dlr_climate_stress_horizon_annual():
    scenarios = dlt.read(f"{_SILVER}.climate_stress_scenarios_daily")
    trust = dlt.read(f"{_SILVER}.dim_country_historical_trust_metadata")
    energy = dlt.read(f"{_GOLD}.fct_energy_demand_daily").filter(
        col("iso_code").isin(_ISOS)
    )
    iso = spark.createDataFrame(_ISO_MAP, "country STRING, iso_code STRING")

    # Exposure pillar: Relative Load Factor = last known generation / country
    # historical peak generation (held at 2025 levels; no invented growth).
    gen_yearly = (
        energy.select(
            col("iso_code"),
            F.year(col("date")).alias("yr"),
            col("generation_twh"),
        )
        .filter(col("generation_twh").isNotNull())
        .distinct()
    )
    peak = gen_yearly.groupBy("iso_code").agg(
        F.max("generation_twh").alias("peak_generation_twh")
    )
    w_last = Window.partitionBy("iso_code").orderBy(col("yr").desc())
    last_gen = (
        gen_yearly.withColumn("rn", F.row_number().over(w_last))
        .filter(col("rn") == 1)
        .select(
            col("iso_code"),
            col("generation_twh").alias("last_generation_twh"),
        )
    )
    exposure = (
        peak.join(last_gen, "iso_code")
        .withColumn(
            "exposure_load_factor",
            col("last_generation_twh") / col("peak_generation_twh"),
        )
        .select("iso_code", "exposure_load_factor")
        .join(iso, "iso_code")
        .select("country", "exposure_load_factor")
    )

    dims = trust.select(
        col("country"),
        col("nominal_base_rating_amps"),
        col("historical_vulnerability_coefficient"),
    )

    daily = (
        scenarios.alias("s")
        .join(dims.alias("dm"), col("s.country") == col("dm.country"))
        .join(exposure.alias("ex"), col("s.country") == col("ex.country"))
    )

    # Choke: static falls more than 5% below the nominal base
    # <=> scenario daily max temperature above 35 + (0.05 x base) / 15.
    choke_temp = lit(35.0) + (
        lit(0.05) * col("dm.nominal_base_rating_amps")
    ) / lit(15.0)

    grouped = daily.groupBy(
        col("s.country").alias("country"),
        col("s.scenario").alias("scenario"),
        F.year(col("s.date")).alias("year"),
        col("dm.nominal_base_rating_amps").alias("base_rating"),
        col("ex.exposure_load_factor").alias("exposure_load_factor"),
        col("dm.historical_vulnerability_coefficient").alias(
            "vulnerability_coefficient"
        ),
    ).agg(
        F.sum(
            F.when(
                col("s.scenario_temp_max_c") > choke_temp, lit(1)
            ).otherwise(lit(0))
        ).alias("throttling_days"),
        F.coalesce(
            F.avg(
                F.when(col("s.temp_anomaly") > lit(0.0), col("s.temp_anomaly"))
            ),
            lit(0.0),
        ).alias("hazard"),
    )

    return grouped.select(
        col("country"),
        col("scenario"),
        col("year"),
        col("throttling_days").cast("int").alias(
            "projected_critical_throttling_days"
        ),
        F.round(col("hazard"), 6).alias("hazard_mean_positive_anomaly_c"),
        F.round(col("exposure_load_factor"), 6).alias("exposure_load_factor"),
        col("vulnerability_coefficient"),
        F.round(
            col("hazard")
            * col("exposure_load_factor")
            * col("vulnerability_coefficient"),
            8,
        ).alias("long_term_grid_vulnerability_score"),
    )
