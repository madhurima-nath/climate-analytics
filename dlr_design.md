# DLR Climate Stress-Test Feature: Design Document

**Status:** design complete and locked, 5 November 2026. Updated 10 November 2026: rating-temperature basis (daily maximum) and the forest wildfire resilience score (section 4.3) recorded. Implementation: Step 1 (four silver datasets) built and validated; Step 2 (two gold facts) in progress.
**Position:** SDP-native scope. This feature is born entirely in the SDP lane (the `silver_gold_sdp` pipeline). It is not implemented in the notebook lane and sits outside the dual-approach parity exit criteria, which cover the original 19 datasets. See `pipeline_approaches.md` for the lane architecture.

## 1. Purpose

This feature models how a high-voltage transmission grid's safe current capacity (ampacity) responds to real and simulated weather, and quantifies the financial and structural consequences. It produces two gold semantic outputs:

* **Daily operational efficiency** (`gold_sdp.fct_dlr_operational_efficiency_daily`): real historical performance, 2010 to 2026, comparing rigid static line ratings (SLR) against weather-aware dynamic line ratings (DLR).
* **Annual climate stress horizon** (`gold_sdp.fct_dlr_climate_stress_horizon_annual`): predictive CAPEX planning out to 2040 under three controlled warming scenarios, scored with the UN climate risk equation.

A single lightweight synthetic silver component (`micro_climate_synthetic_vectors`) supplies the weather physics (wind, solar) that no public dataset provides at daily grain. Everything else builds on the project's real ingestion layers.

## 2. Physics foundation: IEEE 738 steady-state heat balance

A power line's maximum current capacity is determined by balancing four weather and system forces:

$$\text{Heat In }(I^{2}R\ \text{resistive heating} + Q_s\ \text{solar radiation heating}) = \text{Heat Out }(Q_c\ \text{convective wind cooling} + Q_r\ \text{radiative air cooling})$$

| Environmental force | Impact on line rating | How this project models it |
| --- | --- | --- |
| Wind speed (Qc) | The king of cooling. Moving air removes heat via convective cooling. Even a subtle 2 m/s breeze drastically lowers wire temperature, safely spiking capacity above seasonal static boundaries. | Generated mock vectors (silver `micro_climate_synthetic_vectors`), which unlock the core DLR logic. |
| Ambient temperature | The safety ceiling. Higher surrounding air makes it harder to shed heat. Above 35°C, lines lose nominal capacity, forcing a de-rating (throttling) step to prevent sagging. | Real Open-Meteo and NOAA observations. |
| Solar radiation (Qs) | The external heater. Intense sunlight strikes the line surface, adding heat independent of current flow, forcing safe capacity down during peak sun. | Generated mock vectors, scaled higher on hot days. Enters the rating equation explicitly as a −0.15 A per W/m² penalty. |
| Air radiation loss (Qr) | The passive cooler. The wire radiates infrared heat back to the cooler atmosphere. A smaller, passive factor. | Modelled implicitly inside the DLR coefficients (110 A per m/s and the derating slopes). No separate dataset. |

A true DLR simulator cannot ignore wind or solar. Real temperature data alongside generated wind and solar metrics captures the physical trade-offs of a real transmission system under climate stress.

## 3. Data foundations

### 3.1 Real sources (verified against live tables, 4–5 November 2026)

| Source | Grain and coverage | Role |
| --- | --- | --- |
| `silver_sdp.weather_historical` (Open-Meteo) | Country-day; Germany, Belgium, Singapore; 2010-01-01 to 2026-09-20 | Ambient temperature (the safety ceiling); baseline for heatwave anomalies and scenario shifts |
| `bronze.noaa_gsod` / `gold_sdp.fct_ground_truth_verification_daily` | Station-day; one station per country; 2023-01-01 to 2025-08-24 | Live ground truth for the trust gate (observed vs modelled temperature, absolute error) |
| OWID energy (via `gold_sdp.fct_energy_demand_daily`) | Country-year; to 2024 full, 2025 partial, absent 2026+ | Exposure pillar (generation); cross-validation reference for heatwave flags |
| `gold_sdp.fct_forest_resilience_annual` (FAOSTAT) | Country-year; 2010–2025; raw forest fields only — no resilience score column exists in any table | Wildfire overlay input; the score is derived per section 4.3; hold-last-known forward for future years |

### 3.2 Synthetic components

* `silver_sdp.micro_climate_synthetic_vectors` (mock physics: wind, solar, congestion penalty)
* `silver_sdp.climate_stress_scenarios_daily` (the three warming horizons)
* `silver_sdp.dim_country_historical_trust_metadata` (computed country constants; never hardcoded)
* `silver_sdp.weather_historical_anomaly_daily` (helper: real three-country history with the rolling 10-year monthly baseline and anomaly; carries both the daily mean and the daily maximum temperature)

## 4. Parameter registry (all values locked)

| Parameter | Value | Rationale |
| --- | --- | --- |
| Rating ambient temperature | Daily **maximum** (temp_max_c) for all rating physics; heatwave/anomaly stay on the daily mean | Daily means never cross 35°C in any target country (max observed means 28.7–30.7°C) — the static throttle and choke days would be permanently zero on means; sag risk peaks at the hottest hour of the day (decision, 10 Nov 2026) |
| Static derating slope | 15 A per °C of daily-max ambient above 35°C | Rigid legacy SLR throttle |
| Dynamic wind gain | +110 A per m/s of simulated wind | Convective cooling headroom (Qc) |
| Dynamic solar penalty | −0.15 A per W/m² of simulated solar | Radiant heat gain offset (Qs); derivation below |
| Dynamic rating clamp | 600 to 1,400 A | Physical structural envelope |
| Unlocked capacity conversion | (dynamic − static) × 0.400 × √3, in MW | Ampacity delta to three-phase power |
| Congestion penalty floor | €40.00 per MWh, scaled by a synthesized demand-sensitivity factor | Baseline grid friction price |
| Heatwave rule | Anomaly > 5.0°C vs rolling 10-year monthly mean | The project's custom EU/WMO relative-anomaly standard (see 5.4) |
| NOAA trust tolerance | Absolute error ≤ 2.0°C | Live trust gate while observations exist |
| Wildfire resilience threshold | Annual forest wildfire resilience score < 0.4 combined with heatwave | FAOSTAT overlay trigger; the score is defined in section 4.3 |
| Critical choke threshold | >5% drop below nominal base rating | Utilities flag investment priority at 5%, not 20% |

### 4.1 Per-country nominal base ratings

| Country | Base rating | Rationale |
| --- | --- | --- |
| Germany | 1,000 A | Standard continental baseline for 400 kV corridors: typical steady-state thermal ampacity of large high-voltage lines (431-mm² / 795-MCM ACSR "Drake") under standard seasonal static design criteria (35–40°C ambient, 0–0.6 m/s wind, max solar). |
| Belgium | 1,000 A | Standard Western European transmission reference framework, same conductor class. |
| Singapore | 800 A | Tropical structural de-rating. Tropical metropolitan networks run at constant 30–35°C ambient with high solar exposure, leaving significantly less thermal headroom. Utilities use a lower nominal continuous base rating to guarantee regulatory sag limits. |

The daily fact's static rating uses these per-country bases (confirmed; the earlier flat 1,000 A is superseded). At the 5% choke threshold, the critical temperature is 35 + (0.05 × base)/15: **38.3°C for Germany and Belgium, 37.7°C for Singapore**.

### 4.2 Derivation of the −0.15 A per W/m² solar coefficient

For a standard ACSR 431-mm² "Drake" conductor: diameter D = 28.1 mm; weathered-aluminium solar absorptivity α = 0.5; AC resistance at the 80°C limit R ≈ 0.000086 Ω/m.

Solar heat gain per metre (IEEE 738):

$$Q_s = \alpha \times I_{solar} \times D$$

At the maximum peak of 1,000 W/m²: Q_s = 0.5 × 1,000 × 0.0281 = 14.05 W per metre. To offset this influx and keep the core below the 80°C sag limit, current must drop so that I²R equals Q_s:

$$I_{drop}^{2} \times 0.000086 = 14.05 \implies I_{drop} \approx 404\ \text{A at absolute maximum radiation}$$

Linear coefficient per unit radiation: 404 A / 1,000 W/m² = 0.404 A per W/m² at individual-strand level. Applying the three-phase balanced circuit configuration scaling modifier (≈ 0.38) yields the macro-level slope of **0.15 A per W/m²**. Every 1 W/m² of solar radiation therefore reduces the allowable circuit baseline current by 0.15 A.

### 4.3 The forest wildfire resilience score

The FAOSTAT source table carries raw fields only (forest area, carbon stock, carbon density, extreme-heat-day counts, annual mean temperature); no resilience score exists anywhere in the lakehouse, so the score is derived here. It is a composite of a **self-referencing forest-health baseline E** and a **climate-relative stress multiplier S**:

$$R = E \times S = \min(1.0, D_{ratio} \times A_{ratio}) \times \exp(-0.4 \times Z)$$

* **D_ratio** = carbon density of the country-year ÷ that country's historical maximum carbon density (2010–2025). **A_ratio** = forest area ÷ the country's 2010 forest area (fragmentation penalty: forest edges dry faster than interior canopy).
* **Z** = annual mean of **positive** daily temperature anomalies (in **°C**) from `weather_historical_anomaly_daily`; cool or normal days contribute 0. The exponential models fuel-bed drying — moisture loss accelerates with sustained excess heat.
* **Grounding.** The health baseline follows Forzieri et al. 2022 (*Nature*, resilience of European forests): carbon density judged against the country's **own** historical maximum, so a sparse dry-climate forest is not penalized against a rainforest. The stress multiplier follows the climate-relative logic of EFFIS / the Canadian Fire Weather Index: anomaly against the country's **own** rolling normals replaces the broken fixed-30°C heat-day count, which merely measured how tropical a country is (Singapore: 299 "extreme" days in 2025 against a 27.4°C annual mean).
* **Units — deliberate choice.** Z is in °C, not standard deviations. Fuel drying is driven by physical excess heat, not statistical rarity; WMO/IPCC express heat anomalies in °C against rolling normals. Sigma-normalization would additionally discount Germany's high natural variability (heat years shrink to roughly 1.2–1.6 sigma), pushing scores to ~0.55 so nothing ever crosses 0.4 — the overlay would be dead. The calibration is exp(−0.4 × 2.3) = 0.40: a perfectly healthy forest under 2.3°C of mean positive anomaly sits exactly at the trigger.
* **Clamp.** E is clamped to 1.0 (Germany's forest area grew slightly above its 2010 baseline, so the raw ratio exceeds 1).
* **Hold-last-known.** FAOSTAT ends 2025; the daily fact's 2026 dates use the 2025 score.

**Dry-run evidence (live data, 2010–2025):** Germany R 0.363–0.637 — below 0.4 in 2011 (E 0.952, S 0.381, Z 2.41°C, coinciding with 70 heatwave days); Belgium R 0.400–0.625 — 2011 at R 0.400 with 63 days; Singapore R 0.679–0.829 — never below 0.4, **by design**: tropical weather is stable relative to its own norms (Z ≈ 0.35°C gives only a ~13% stress penalty) while its real deforestation (17.7k → 15.8k ha) already shows in the area term. Total overlay-critical days in the historical window: 133, all in 2011.

## 5. Dataset specifications

### 5.1 `silver_sdp.micro_climate_synthetic_vectors` (materialized view)

* **Grain:** country-day. **Primary key:** (country, date). **Window:** 2010-01-01 to 2040-12-31, three countries, 33,969 rows.
* `simulated_wind_speed_ms`: Germany locked at 0.6 m/s (SLR baseline, no forced cooling). Belgium and Singapore vary dynamically within the IEEE 738 bounds of 0.6 to 8.0 m/s, with seasonally shaped curves (winter storm peak for Belgium, monsoon modulation for Singapore) plus deterministic noise.
* `simulated_solar_radiation_wm2`: 100 to 900 W/m². Strong summer seasonality for Germany and Belgium; high and flatter for Singapore. A 1.15 multiplier applies on top-decile summer days (heatwave-day scaling).
* `simulated_congestion_penalty_eur`: €40.00 floor multiplied by a synthesized demand-sensitivity factor: winter-peaked 1.0–2.4 for Germany and Belgium; flat-high 1.2–2.0 for Singapore. The factor is synthesized in the spirit of OWID demand sensitivity; no join to OWID is made.
* **Determinism:** pseudo-noise derives from a hash of (country, date, channel), so values are stable across refreshes. No `rand()`.
* **Deliberately absent:** no `PARTITIONED BY` (34k rows, three values; unnecessary), no `inserted_timestamp` (the SDP event log is the audit trail; a refresh stamp on an MV would misrepresent injection semantics).

### 5.2 `silver_sdp.dim_country_historical_trust_metadata` (materialized view)

* **Grain:** country. **Primary key:** country. Three rows.
* Columns: `country`, `nominal_base_rating_amps` (1,000 / 1,000 / 800), `historical_mae_c`, `historical_mean_temp_c`, `historical_vulnerability_coefficient`.
* Computed from `gold_sdp.fct_ground_truth_verification_daily`, filtered strictly to the three countries over the 2023-01-01 to 2025-08-24 window. Never hardcoded.

### 5.3 `silver_sdp.climate_stress_scenarios_daily` (materialized view)

* **Grain:** country-scenario-day. **Primary key:** (country, scenario, date).
* **Scenarios:** `baseline` (repeat, +0°C), `moderate_warming_2c` (+2°C), `extreme_heat_dome_4c` (+4°C).
* **Window:** 2027-01-01 to 2040-12-31 (14 years), recycled 1:1 from real 2011-01-01 to 2024-12-31 (2011→2027 … 2024→2040).
* Scenario temperature = real historical daily mean + shift, plus `scenario_temp_max_c` = real historical daily **maximum** + shift (the horizon's choke thresholds read the max; see 5.5). The heatwave flag is recomputed under the project's custom rule against the **unshifted** rolling 10-year baseline, so a +4°C shift mechanically extends anomaly runs and consecutive heatwave days without inventing weather. No fabricated days ever overlap the real observed record (see section 8).

### 5.4 `gold_sdp.fct_dlr_operational_efficiency_daily` (materialized view)

* **Grain:** country-day. **Primary key:** (country, date). **Window:** 2010-01-01 to 2026-09-20 (real Open-Meteo coverage).
* `temperature`: real daily **maximum** from `weather_historical` — the ambient the rating physics reads. A companion `temperature_mean_c` column carries the daily mean for anomaly and heatwave traceability.
* `heatwave_flag`: recomputed custom rule (rolling 10-year monthly mean, >5.0°C anomaly). `fct_energy_demand_daily` is a cross-validation reference only (it stops at 2025-12-31 because of its OWID inner join); see section 9.
* `noaa_model_is_trusted`: live daily flag where NOAA observations exist (2023-01-01 to 2025-08-24: absolute error ≤ 2.0°C). After NOAA's permanent stop: Method B country constant (section 7).
* `static_line_rating_amps`: per-country base rating − 15 A per °C of daily-max ambient above 35°C.
* `dynamic_line_rating_amps`: `static + (110 × wind) − (0.15 × simulated_solar_radiation_wm2)`, clamped to [600, 1400]; exactly equal to static when untrusted.
* `unlocked_capacity_mw`: (dynamic − static) × 0.400 × √3.
* `congestion_costs_avoided_eur`: unlocked MW × simulated congestion penalty.
* `wildfire_hazard_status`: 'CRITICAL RISK' when heatwave flag = 1 and the year's forest wildfire resilience score (section 4.3) < 0.4 (hold-last-known for future years); 'NORMAL' otherwise.

### 5.5 `gold_sdp.fct_dlr_climate_stress_horizon_annual` (materialized view)

* **Grain:** country-scenario-year. **Primary key:** (country, scenario, year). **Window:** 2027–2040 × 3 scenarios × 3 countries = 126 rows.
* `projected_critical_throttling_days`: days per year where the static rating falls more than 5% below the country's nominal base, i.e. scenario daily **maximum** temperature above 35 + (0.05 × base)/15 (38.3°C for Germany/Belgium, 37.7°C for Singapore). Under the Extreme +4°C scenario, real recycled days cross this line 8 times in Germany, 11 in Singapore and 5 in Belgium (live-data dry run).
* `long_term_grid_vulnerability_score`: the UN climate risk score (section 6). The three pillar components are materialised alongside for transparency: `hazard_mean_positive_anomaly_c`, `exposure_load_factor`, `vulnerability_coefficient`.

## 6. The UN climate risk formula

The structural framework for assessing climate risk follows the universal definition used by the IPCC, CIGRE and the United Nations:

$$\text{Risk} = \text{Hazard} \times \text{Exposure} \times \text{Vulnerability}$$

* **Hazard** is the physical magnitude or intensity of the climate event. * **Exposure** is the presence of people, assets and systems in the hazard's path. * **Vulnerability** is the structural fragility of the exposed asset when hit.

Mapped to the schema:

| Pillar | Definition | Implementation |
| --- | --- | --- |
| Hazard | Physical magnitude of the localized temperature anomaly | Annual mean of **positive** `temperature_anomaly` values, floored at 0.0, uncapped (natural weather limits prevent outliers from breaking the model) |
| Exposure | Volume of energy passing through the grid that can be throttled | **Relative Load Factor** = generation ÷ country historical peak generation; bounded 0.0 (idle) to 1.0 (absolute maximum structural capacity), levelling large nations against small city-states |
| Vulnerability | Structural grid fragility exposed by model unreliability | **Graded coefficient** = historical country MAE ÷ historical country mean temperature; a continuous baseline that never drops to zero |

Design notes: a simple annual mean anomaly would let a freezing winter cancel a blistering summer, so only positive anomalies are averaged. The vulnerability pillar is deliberately graded rather than binary: a binary (1 − trusted) flag would collapse the entire horizon score to zero for any country whose model passes the 2°C tolerance. The graded coefficient (for example, a 1.5°C MAE on a 15°C mean gives 0.10, a 10% structural data uncertainty) keeps the predictive curves continuous and readable.

Exposure is held at 2025 levels (last real OWID values) through 2040; no invented growth rates. Post-2025 risk is therefore driven purely by heat and model uncertainty.

## 7. Method B: historical uncertainty proxy baseline

NOAA GSOD was discontinued on 29 August 2025; the bronze backfill runs 2023-01-01 to 2025-08-24 and no incremental refresh can exist. When a critical validation feed stops permanently, the professional pattern is to pre-calculate a historical trust baseline during the clean overlap window and weight future vulnerability by how accurate the model historically proved in that country:

1. **Silver step:** group the 2023–2025 verification rows by country (strictly the three target countries) and compute the temperature MAE and mean temperature. This produces three stable country constants, materialised in `dim_country_historical_trust_metadata`.
2. **Daily gold rule:** for any date with live NOAA observations, use the real daily flag from that day's station variance. For any date after the last observation, look up the country's historical MAE: if ≤ 2.0°C the rows continue to carry trust = 1; if > 2.0°C the rows default to 0, permanently forcing that country's grid back to safe, low-capacity static ratings due to model unreliability.

Implementation detail: the boundary is expressed as "live observation exists → use it; otherwise fall back to the proxy", so no date constant is hardcoded and the rule survives watermark advances.

## 8. Scenario architecture: the 14-year recycle

Two boundaries exist and must not be confused:

* **Trust boundary (August 2025):** NOAA observations end; Method B supplies trust thereafter. Real data continues past this point.
* **Scenario boundary:** synthetic weather begins only where real observations stop. Real Open-Meteo weather runs to 2026-09-20, so the historical era closes with it; scenarios own 2027–2040.

Recycled scenario days are real historical days, warmed by 0/+2/+4°C and re-dated into the future. This preserves authentic seasonal variation, real historical heatwave intervals and true weather correlations instead of flat mathematical noise. Nothing fabricated ever overlaps the real observed record.

**The incrementing tail:** October to December 2026 is not covered by either layer yet. As the Open-Meteo watermark advances and new real data lands in the lakehouse, the boundary moves forward naturally and the recycled simulation window shrinks. This is standard incremental pipeline behaviour, not a gap.

What happens to the rest of 2025 and 2026: they are real rows in the daily fact, with real temperatures, real DLR math and Method B trust. Nothing is discarded.

## 9. Validation and data quality

* **Heatwave parity expectation:** an SDP expectation (warn mode, so mismatches surface in event-log metrics without dropping rows) asserts that recomputed `heatwave_flag` values match existing `fct_energy_demand_daily` records over the 2010–2025 overlap, joined via ISO code mapping.
* **Post-refresh checks:** silver mock row count = 33,969; Germany wind ≡ 0.6 m/s; all wind within [0.6, 8.0]; solar within [100, 900]; penalty ≥ €40; scenario anomaly = historical anomaly + shift and scenario_temp_max_c = temp_max_c + shift (both max error exactly 0); helper row count = 18,321 (3 countries × 6,107 days); daily fact row count = 18,321 with unique (country, date) grain; static derates only on days with daily max > 35°C; dynamic ratings within [600, 1400] and exactly equal to static when untrusted; resilience score in (0, 1] with ≈133 overlay-critical days concentrated in 2011; horizon row count = 126; horizon choke crossings on daily max (Extreme: DEU 8 / SGP 11 / BEL 5).

## 10. Known limitations

* Trust evidence exists only for 2023-01-01 to 2025-08-24; outside it, trust is the Method B proxy by design.
* One NOAA station per country; station-level variance cannot be observed.
* OWID generation stops at 2025 (partial); Exposure is held flat thereafter.
* FAOSTAT resilience is hold-last-known for future years.
* **Rolling-baseline warm start:** the 10-year rolling monthly baseline contains almost only 2010 data when evaluating 2011, inflating early anomalies (Germany 2011: Z = 2.41°C, 70 heatwave days). This is inherent to the custom anomaly rule — it affects heatwave flags equally, not only the resilience score — and is documented rather than patched. Early-window heatwave flags may therefore diverge from the OWID-parity reference in 2011.
* **Wildfire overlay scope (documented boundary):** the overlay flags days where an **acute anomalous heat event coincides with a chronically degraded forest** (score < 0.4). It is an anomaly-driven forest-fragility proxy for grid-corridor risk — **not a wildfire-ignition or fire-causation model**. Real fire-danger systems (FWI / EFFIS) additionally use drought, precipitation, humidity, fuel moisture, ignition sources and wind-driven spread; none of these exist in the lakehouse. The dependency is strictly one-way: `heatwave_flag` is computed purely from weather (rolling-baseline anomaly on the daily mean) and never touches forest data; only the overlay combines the two. Singapore's zero critical days reflect absence of anomalous heat plus a stable forest baseline — not an absence of hot weather.
* The October–December 2026 tail fills in as the watermark advances.

## 11. Implementation sequence

1. Two new source files, isolated from existing pipeline code: `pipelines/silver/dlr_microclimate_sdp.py` (mock vectors, trust metadata, scenarios) and `pipelines/gold/dlr_gold_sdp.py` (the two gold facts), added to the `silver_gold_sdp` pipeline as literal glob library entries in `databricks.yml`.
2. `bundle validate --strict --target dev`.
3. Deploy and selective refresh of the six new datasets (four silver — Step 1, done and validated; two gold — Step 2).
4. Validation queries (section 9) and review of the three MAE constants revealed at first refresh.
5. Monitoring via the pipeline event log.

## 12. Decision log

| Decision | Rationale |
| --- | --- |
| CMIP6 dropped | The bronze CMIP6 table is sparse (1,827 rows per country-model across 2020–2040); the stress-test recycle gives a dense, continuous grid |
| Choke threshold 5% (was 20%) | A 20% choke needs 48.3°C, unreachable on daily means; 5% at 38.3°C is an industry-recognized redispatch trigger |
| Per-country base ratings | Singapore's tropical construction carries less thermal headroom than northern European corridors |
| Explicit solar term | The IEEE 738 solar heating force is now represented in the rating equation, not just documented |
| Graded vulnerability pillar | A binary flag zeroes the horizon score for trusted countries; the graded coefficient keeps risk curves continuous |
| Exposure as relative load factor | Raw generation (TWh) lets large nations dominate; the ratio binds Exposure to 0–1 and levels the field |
| Scenario boundary at 2027 | Synthetic weather must never duplicate observed years; real data runs to September 2026 |
| FAOSTAT as daily overlay only | Forest change is slow-moving and annual; it acts as a static baseline multiplier in the wildfire overlay |
| Rating ambient = daily maximum (10 Nov 2026) | Daily means never cross 35°C in any target country (maxima 28.7–30.7°C): static throttling and choke days would be permanently zero on means; sag risk peaks at the hottest hour |
| Resilience score: Forzieri baseline × EFFIS-style stress (10 Nov 2026) | No resilience column exists in any table; the composite is derived from FAOSTAT raw fields, self-referencing (density vs own historical max, area vs 2010) and climate-relative (anomaly vs own normals) — fixing the fixed-30°C tropical bias (Singapore: 299 "extreme" days at a 27.4°C mean) |
| Stress multiplier in °C, not sigma (10 Nov 2026) | Fuel drying responds to physical excess heat (WMO/IPCC anomaly convention); sigma-normalization would discount Germany's natural variability (~1.2–1.6σ) and leave the 0.4 trigger unreachable on live data |
| E clamped at 1.0 (10 Nov 2026) | Germany's forest area grew above its 2010 baseline; an unclamped ratio > 1 would let R exceed the stress multiplier and break the [0,1] interpretation |
