"""
Köppen-Geiger climate classification and degree-day base temperature lookup.

Classification follows Kottek et al. (2006) and Peel et al. (2007).
All precipitation seasonality assumes Northern Hemisphere (Apr-Sep = summer, Oct-Mar = winter).
This can be refined for Southern Hemisphere when station coordinates become available.
"""

from pyspark.sql.functions import udf, month as spark_month, col, lit, count, sum as spark_sum, avg as spark_avg, when, collect_list
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType


# ============================================================================
# BASE TEMPERATURE LOOKUP BY KÖPPEN ZONE
# ============================================================================
# Agreed thresholds — heating/cooling degree-day base temperatures (°C).
# A (Tropical): no heating, cooling only above 28°C
# B (Arid):     heating at 18°C, cooling above 25°C
# C (Temperate): heating at 15°C, cooling above 25°C
# D (Continental): heating at 15°C, cooling above 25°C
# E (Polar):    heating at 12°C, no cooling

KOPPEN_BASE_TEMPS = {
    'A': {'heating_base_temp': 0,  'cooling_base_temp': 28},
    'B': {'heating_base_temp': 18, 'cooling_base_temp': 25},
    'C': {'heating_base_temp': 15, 'cooling_base_temp': 25},
    'D': {'heating_base_temp': 15, 'cooling_base_temp': 25},
    'E': {'heating_base_temp': 12, 'cooling_base_temp': 0},
}

# ============================================================================
# KÖPPEN ZONE DESCRIPTIONS
# ============================================================================

KOPPEN_DESCRIPTIONS = {
    'Af':  'Tropical rainforest',
    'Am':  'Tropical monsoon',
    'Aw':  'Tropical savanna',
    'BWh': 'Hot desert',
    'BWk': 'Cold desert',
    'BSh': 'Hot steppe',
    'BSk': 'Cold steppe',
    'Cfa': 'Humid subtropical',
    'Cfb': 'Oceanic',
    'Cfc': 'Subpolar oceanic',
    'Csa': 'Hot-summer Mediterranean',
    'Csb': 'Warm-summer Mediterranean',
    'Csc': 'Cool-summer Mediterranean',
    'Cwa': 'Humid subtropical (dry winter)',
    'Cwb': 'Subtropical highland',
    'Cwc': 'Cold subtropical highland',
    'Dfa': 'Hot-summer humid continental',
    'Dfb': 'Warm-summer humid continental',
    'Dfc': 'Subarctic',
    'Dfd': 'Extremely cold subarctic',
    'Dwa': 'Hot-summer continental (dry winter)',
    'Dwb': 'Warm-summer continental (dry winter)',
    'Dwc': 'Subarctic (dry winter)',
    'Dwd': 'Extremely cold subarctic (dry winter)',
    'ET':  'Tundra',
    'EF':  'Ice cap',
}


def get_base_temps(koppen_code):
    """Return (heating_base_temp, cooling_base_temp) for a Köppen zone."""
    if not koppen_code or len(koppen_code) < 1:
        return None, None
    zone = KOPPEN_BASE_TEMPS.get(koppen_code[0])
    if zone is None:
        return None, None
    return zone['heating_base_temp'], zone['cooling_base_temp']


def get_koppen_description(koppen_code):
    """Return human-readable description for a Köppen zone code."""
    if not koppen_code:
        return None
    return KOPPEN_DESCRIPTIONS.get(koppen_code, f'Unknown zone: {koppen_code}')


# ============================================================================
# KÖPPEN-GEIGER CLASSIFICATION ALGORITHM
# ============================================================================

def koppen_classify(monthly_temps, monthly_precips):
    """
    Classify climate using the Köppen-Geiger system (Kottek et al. 2006).

    Args:
        monthly_temps:   list of 12 monthly mean temperatures (°C), Jan–Dec
        monthly_precips:  list of 12 monthly total precipitation (mm), Jan–Dec

    Returns:
        3-character Köppen-Geiger code (e.g., 'Cfb', 'BWh', 'Af'), or None.
    """
    if monthly_temps is None or monthly_precips is None:
        return None
    if len(monthly_temps) < 12 or len(monthly_precips) < 12:
        return None

    T = list(monthly_temps)
    P = list(monthly_precips)

    # Annual and extreme values
    T_annual = sum(T) / 12.0
    P_annual = sum(P)
    T_hot = max(T)
    T_cold = min(T)
    T_mon10 = sum(1 for t in T if t >= 10)   # months with temp >= 10°C

    # Seasonal precipitation (Northern Hemisphere)
    P_summer = sum(P[3:9])                   # Apr–Sep
    P_winter = sum(P[0:3]) + sum(P[9:12])    # Jan–Mar + Oct–Dec

    # --- B climate threshold (Peel et al. 2007) ---
    if P_winter >= 0.7 * P_annual:
        P_threshold = 20 * T_annual
    elif P_summer >= 0.7 * P_annual:
        P_threshold = 20 * T_annual + 280
    else:
        P_threshold = 20 * T_annual + 140

    # --- First letter: main climate ---
    # Order matters: E and B are checked before A/C/D.
    # B (arid) overrides A (tropical) for hot-but-dry regions like Dubai.
    if T_hot < 10:
        main = 'E'
    elif P_annual < P_threshold:
        main = 'B'
    elif T_cold >= 18:
        main = 'A'
    elif T_cold >= -3:
        main = 'C'
    else:
        main = 'D'

    # --- Second and third letters ---
    if main == 'A':
        P_driest = min(P)
        if P_driest >= 60:
            sub = 'f'
        elif P_annual >= 25 * (100 - P_driest):
            sub = 'm'
        else:
            sub = 'w'
        return main + sub

    if main == 'B':
        if P_annual < P_threshold / 2:
            sub = 'W'
        else:
            sub = 'S'
        third = 'h' if T_annual >= 18 else 'k'
        return main + sub + third

    if main in ('C', 'D'):
        P_driest_summer = min(P[3:9])
        P_wettest_winter = max(P[0:3] + P[9:12])
        P_driest_winter = min(P[0:3] + P[9:12])
        P_wettest_summer = max(P[3:9])

        if P_driest_summer < 40 and P_wettest_winter > 0 and P_driest_summer < P_wettest_winter / 3:
            sub = 's'
        elif P_wettest_summer > 0 and P_driest_winter < P_wettest_summer / 10:
            sub = 'w'
        else:
            sub = 'f'

        if T_hot >= 22:
            third = 'a'
        elif T_mon10 >= 4:
            third = 'b'
        else:
            third = 'c'

        if main == 'D' and T_cold < -38:
            third = 'd'

        return main + sub + third

    if main == 'E':
        sub = 'T' if T_hot >= 0 else 'F'
        return main + sub

    return None


# ============================================================================
# PYSPARK UDF WRAPPERS
# ============================================================================

koppen_classify_udf = udf(koppen_classify, StringType())


# ============================================================================
# GOLD DIM TRANSFORM FUNCTIONS
# ============================================================================
# These functions are called by the gold pipeline orchestrator via YML configs.
# Each follows the signature: (sources: dict, params: dict) -> DataFrame

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import row_number

# --- Data quality threshold ---
# Minimum fraction of non-NULL precipitation days per month for reliable classification
MIN_PRECIP_COVERAGE = 0.70

# --- UDF: Köppen classification with data quality check ---
_koppen_result_schema = StructType([
    StructField("koppen_zone", StringType(), True),
    StructField("koppen_description", StringType(), True),
    StructField("heating_base_temp", IntegerType(), True),
    StructField("cooling_base_temp", IntegerType(), True),
    StructField("data_quality_flag", StringType(), True),
])

def _koppen_with_dq(months, temps, precips, coverage_flags):
    """
    Classify Köppen with data quality check.
    Sorts arrays by month (Jan–Dec), verifies coverage, classifies if sufficient.
    Returns struct: (zone, description, heating_base, cooling_base, flag).
    """
    if not months or len(months) != 12:
        return (None, None, None, None, "Insufficient: missing months")

    combined = sorted(zip(months, temps, precips, coverage_flags))
    sorted_temps = [t for _, t, _, _ in combined]
    sorted_precips = [p for _, _, p, _ in combined]
    sorted_coverage = [c for _, _, _, c in combined]

    if not all(sorted_coverage):
        return (None, None, None, None, "Insufficient precipitation data")

    if any(t is None for t in sorted_temps) or any(p is None for p in sorted_precips):
        return (None, None, None, None, "Insufficient: NULL values in monthly data")

    zone = koppen_classify(sorted_temps, sorted_precips)
    desc = KOPPEN_DESCRIPTIONS.get(zone)
    hdd, cdd = get_base_temps(zone) if zone else (None, None)
    return (zone, desc, hdd, cdd, "Sufficient")

_koppen_with_dq_udf = udf(_koppen_with_dq, _koppen_result_schema)


def _compute_koppen(weather_df, group_col):
    """
    Aggregate weather data by group_col (station_id or country), compute
    Köppen classification with data quality flag.
    Returns DataFrame: [group_col, koppen_zone, koppen_description,
                        heating_base_temp, cooling_base_temp, data_quality_flag]
    """
    monthly = weather_df.groupBy(
        col(group_col), spark_month(col("date")).alias("month")
    ).agg(
        spark_avg(col("temp_mean_c")).alias("avg_temp"),
        spark_sum(col("precip_mm")).alias("total_precip"),
        count("precip_mm").alias("measured_days"),
        count(lit(1)).alias("total_days"),
    )

    monthly = monthly.withColumn(
        "has_coverage",
        (col("measured_days") / col("total_days")) >= lit(MIN_PRECIP_COVERAGE),
    )

    grouped = monthly.groupBy(col(group_col)).agg(
        collect_list("month").alias("months"),
        collect_list("avg_temp").alias("temps"),
        collect_list("total_precip").alias("precips"),
        collect_list("has_coverage").alias("coverage_flags"),
    )

    result = grouped.withColumn(
        "koppen_result",
        _koppen_with_dq_udf(
            col("months"), col("temps"), col("precips"), col("coverage_flags")
        ),
    )

    return result.select(
        col(group_col),
        col("koppen_result.koppen_zone").alias("koppen_zone"),
        col("koppen_result.koppen_description").alias("koppen_description"),
        col("koppen_result.heating_base_temp").alias("heating_base_temp"),
        col("koppen_result.cooling_base_temp").alias("cooling_base_temp"),
        col("koppen_result.data_quality_flag").alias("data_quality_flag"),
    )


# -----------------------------------------------------------------------------
# dim_koppen_zones — static reference table generated from Python dicts
# -----------------------------------------------------------------------------
def transform_dim_koppen_zones(sources: dict, params: dict) -> DataFrame:
    """
    Generate Köppen-Geiger reference table with zone codes, descriptions,
    and climate-zone-specific degree-day base temperatures.
    No silver source needed — data comes from KOPPEN_DESCRIPTIONS and KOPPEN_BASE_TEMPS.
    """
    spark = SparkSession.getActiveSession()

    rows = []
    for zone_code, desc in KOPPEN_DESCRIPTIONS.items():
        hdd, cdd = get_base_temps(zone_code)
        rows.append((zone_code, desc, hdd, cdd))

    schema = StructType([
        StructField("koppen_zone", StringType(), False),
        StructField("koppen_description", StringType(), True),
        StructField("heating_base_temp", IntegerType(), True),
        StructField("cooling_base_temp", IntegerType(), True),
    ])

    return spark.createDataFrame(rows, schema)


# -----------------------------------------------------------------------------
# dim_date — add heating/cooling season flags
# -----------------------------------------------------------------------------
def transform_gold_dim_date(sources: dict, params: dict) -> DataFrame:
    """
    Enrich silver dim_date with heating_season and cooling_season boolean flags.
    Heating season: Oct–Mar (Northern Hemisphere convention).
    Cooling season: Jun–Sep.
    """
    df = sources["dim_date_source"]

    return df.withColumn(
        "heating_season",
        spark_month(col("date")).isin([10, 11, 12, 1, 2, 3]),
    ).withColumn(
        "cooling_season",
        spark_month(col("date")).isin([6, 7, 8, 9]),
    )


# -----------------------------------------------------------------------------
# dim_locations — Köppen zone via station-level classification + mode per country
# -----------------------------------------------------------------------------
def transform_gold_dim_locations(sources: dict, params: dict) -> DataFrame:
    """
    Enrich dim_locations with Köppen climate zone, description, dynamic base temps,
    and data quality flag.

    Approach: classify each station individually using local climate data,
    then take the most common (mode) zone across stations in each country.
    This avoids the error of averaging weather across geographically diverse
    countries and then classifying the average.

    Countries where all stations have insufficient precipitation coverage
    are flagged rather than misclassified.
    """
    location_df = sources["dim_locations_source"]
    weather_df = sources["weather_source"]

    # Step 1: Classify each station individually (local climate data)
    station_koppen = _compute_koppen(weather_df, "station_id")

    # Step 2: Map stations to countries
    station_country = weather_df.select("station_id", "country").dropDuplicates(["station_id"])

    # Step 3: Join zones with country, keep only sufficient classifications
    classified = (
        station_koppen.join(station_country, on="station_id", how="inner")
        .filter(col("data_quality_flag") == "Sufficient")
    )

    # Step 4: Find most common (mode) Köppen zone per country
    zone_counts = classified.groupBy(
        "country", "koppen_zone", "koppen_description",
        "heating_base_temp", "cooling_base_temp"
    ).count()

    w = Window.partitionBy("country").orderBy(col("count").desc())
    country_zone = (
        zone_counts
        .withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn", "count")
    )

    # Step 5: Get all countries that have weather data (for flag)
    weather_countries = weather_df.select(
        col("country").alias("wc_country")
    ).distinct()

    # Step 6: Join everything to dim_locations
    result = (
        location_df
        .join(country_zone, location_df["country_name"] == country_zone["country"], how="left")
        .join(weather_countries, location_df["country_name"] == weather_countries["wc_country"], how="left")
    )

    # Step 7: Set data quality flag
    result = result.withColumn(
        "data_quality_flag",
        when(col("koppen_zone").isNotNull(), lit("Sufficient"))
        .when(col("wc_country").isNotNull(), lit("Insufficient: no classified stations"))
        .otherwise(lit("No weather data")),
    )

    return result.select(
        col("country_name"),
        col("iso_code"),
        col("fao_code"),
        col("koppen_zone"),
        col("koppen_description"),
        col("heating_base_temp"),
        col("cooling_base_temp"),
        col("data_quality_flag"),
    )


# -----------------------------------------------------------------------------
# dim_stations — add Köppen zone and data quality flag
# -----------------------------------------------------------------------------
def transform_gold_dim_stations(sources: dict, params: dict) -> DataFrame:
    """
    Enrich dim_stations with Köppen climate zone, description, and data quality flag
    from per-station weather observations.

    Stations with insufficient precipitation coverage (<70% of days per month
    having non-NULL precipitation) are flagged rather than misclassified.
    """
    stations_df = sources["dim_stations_source"]
    weather_df = sources["weather_source"]

    # Compute Köppen per station from weather_observations
    koppen_df = _compute_koppen(weather_df, "station_id")

    # Join back to dim_stations
    result = stations_df.join(koppen_df, on="station_id", how="left")

    # Flag stations with no weather data at all
    result = result.withColumn(
        "data_quality_flag",
        when(col("data_quality_flag").isNull(), lit("No weather data"))
        .otherwise(col("data_quality_flag")),
    )

    return result.select(
        col("station_id"),
        col("station_name"),
        col("country"),
        col("koppen_zone"),
        col("koppen_description"),
        col("data_quality_flag"),
    )