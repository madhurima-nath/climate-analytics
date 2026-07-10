# Week 2 Course Notes: Applying Climate Data
## Climate Intelligence — University of Reading (FutureLearn)

---

## Types of Climate Data

No single data source is perfect. All have advantages and limitations. The key question when selecting data is whether it is "fit for purpose" for the specific decision being made.

### Site-Based Meteorological Instruments

The most conceptually straightforward source: dedicated instruments recording temperature, precipitation, wind speed, etc. at a fixed location. Where well-sited, well-maintained, and well-calibrated, they provide high quality records of local weather over long periods. Many networks have been in place for decades.

**Limitations:**
- Sites may be sparsely located or in poor positions relative to the area of interest
- Surrounding conditions change over time (tree growth, new buildings), creating spurious trends
- Instruments are replaced over time, introducing discontinuities
- Records can only be created in real time — retrospective historical records cannot be constructed

The course illustrates this with a wind farm developer needing wind speeds over a hill. Two nearby masts (one near a city, one near the coast) are both on the surrounding plains — neither gives a reliable estimate for the target location.

### Remote Sensing and Proxy Observations

Satellite-based observations provide comprehensive coverage across large areas and have revolutionised weather forecasting. Proxy data (tree rings, ice cores) provide long-term perspectives on climate change and variability.

**Limitations:**
- Rely on calibration models that translate observed signals (electromagnetic emissions, isotopic ratios) into climate estimates — assumptions that are often hard to test
- Backward-looking: reveal only the single weather history that actually occurred, not alternative possible histories or future change

### Numerical Models and Simulations

Mathematical models of the climate system used for operational weather and climate forecasting (days to months ahead) and climate change projections (decades ahead). Can produce comprehensive, 3-dimensional gridded simulations at global or regional scale. Can also generate alternative weather scenarios — realisations of weather consistent with historical climate drivers but different from recorded weather, useful for examining rare and extreme events.

**Limitations:**
- Subject to biases and deficiencies that are "difficult or even impossible to fully evaluate"
- Computationally expensive
- Archives exist (CORDEX, CMIP, UK Climate Projections) but may require further processing

### Reanalyses

Comprehensive, 3-dimensional, gridded, historic weather datasets with global coverage spanning several decades. Reanalyses combine numerical weather prediction models with historical observations through **data assimilation** — blending the two to produce a single best estimate of the atmospheric state at each timestep, repeated forward through time.

The data assimilation process: observations and a short-range model forecast are both estimates of the true atmospheric state. Neither is perfect. The DA process blends them to produce an overall best estimate, which then becomes the starting point for the next short-range forecast. This is repeated stepwise.

Two broad types: modern-era reanalyses (roughly 1950-1980 onwards, using the full range of observational systems) and longer-coverage reanalyses (from around 1900 onwards, using a reduced observation set).

**Limitations:**
- Limited resolution (grid box size, typically tens of kilometres) — cannot capture highly localised conditions
- Changes in observational systems over time can introduce spurious trends
- NWP model deficiencies can introduce biases, particularly for surface properties such as precipitation

---

## How to Decide Which Information You Need

The key to selecting the right data lies in clearly defining the specific climate risk problem or decision. Ask:

1. What question, concern or challenge am I trying to address?
2. Can this be expressed as a decision I need to make?
3. Can I define a quantitative metric that encapsulates the problem?

Understanding the data means knowing: the nature of the data being used, how it has been processed, the extent to which it has been validated, and whether it is fit for the specific problem. "Climate data can be quite misleading if it's not handled appropriately."

---

## Principles of Climate Models

Source: video transcript — Principles of climate models.

To create a climate model, the Earth is divided into a grid, typically around 100 sq km per box. Each box holds climate information relevant to its location: land type, vegetation, urban areas. These all contribute to the climate system and affect surrounding areas.

The size of the boxes is a trade-off: smaller boxes capture more local detail but require more computations; larger boxes reduce computational cost but lose resolution.

The further into the future a projection goes, the more elements of the Earth's climate system the model must include: global oceans, sea ice, interactive vegetation (which can change with the climate), atmospheric chemistry, man-made pollution sources, and carbon transfer between the atmosphere, oceans, and land surface.

All elements are written in computer code and run on a large supercomputer. Given initial conditions, the model runs forward in time to produce climate projections for any chosen timescale. Climate models allow us to understand the past, present, and future climate, and to determine how natural and man-made influences affect it.

**GCMs vs NWP models:** Climate models (General Circulation Models) include detailed physical representations of slowly evolving components such as the ocean and sea-ice. Numerical weather prediction (NWP) models do not — ocean and sea-ice evolve on timescales of weeks to decades, far beyond the NWP forecast window, so including them adds computational cost with no practical benefit for short-range forecasting. Both types use equations of motion, parameterisation schemes (for radiation, clouds, surface exchange), and a land surface representation.

---

## Types of Predictability

Source: video transcript and course notes — Types of predictability.

When using climate information to anticipate future conditions, it is important to understand both the type and the source of predictability being used.

### Type 0: Climatology

Derived from the statistical properties of a system where there are no changes to the boundary conditions. A historic climatological prediction — the assumption that the future climate distribution looks like the past. Used when neither current atmospheric state nor forced change is the basis for the prediction.

*Example: an insurance company estimating the 1-in-10 year storm wind speed from 40 years of historical data.*

**Key limitation:** if the climate is actually changing, historical statistics are an unreliable guide to future risk — particularly at the extremes of the distribution.

### Type I: Initial Condition Predictability

Relies on knowing the current state of the climate accurately and tracking how it evolves. Classic example: a day-ahead weather forecast.

The pegboard analogy: each ball represents a weather day, each bucket a weather outcome. Knowing where the ball is dropped (the initial condition) determines which bucket it falls into. The more accurately the initial conditions are known, the more accurately the outcome can be predicted.

**Hard limit:** atmospheric chaos (the Butterfly Effect) means this predictability degrades to near-zero after approximately several days, regardless of how well the starting state is known. Predictability of this type is typically limited to within a few days of the forecast launch.

*Example: Airspace Unlimited using 72-hour winds-aloft forecasts.*

### Type II: Boundary Condition Predictability

Relies on detecting a change produced by some externally applied forcing — such as increased greenhouse gas concentrations. The concern is not predicting an individual weather event but how the overall distribution of weather outcomes shifts.

The pegboard analogy: tipping the board changes which side the balls tend to fall toward. The forcing tilts the system; the individual ball paths remain uncertain.

*Example: Historic England using high emissions scenarios to project how the distribution of overheating hazards will shift to 2060 and beyond.*

**Key challenge:** slowly varying natural components of the climate system (30-year ocean cycles, etc.) can obscure or exacerbate the forced signal. Over a 25-year sample, a genuine long-term warming trend can appear as a cooling trend due to natural decadal variability. Long data records are needed to separate forced change from natural variability.

### The Middle Ground: S2S (Sub-Seasonal to Seasonal) Forecasting

Beyond a few days, Type I predictability from the fast atmosphere is lost. But the climate system contains components that evolve more slowly: oceans, land surface, upper atmosphere. Their initial conditions retain predictability over weeks to months and act as boundary conditions on the faster atmosphere above them.

S2S forecasting exploits this: the initial state of a slow component (for example, a particular pattern in the winter stratosphere) propagates a statistical influence on surface weather behaviour over the following months.

Key properties of S2S forecasts:
- Probabilistic — they describe likelihoods, not certainties
- Apply over larger areas and longer periods than weather forecasts
- Skill relates to the distribution of outcomes, not individual events

*Example: a forecast for European area-average surface temperature for the season ahead — mixing Type I (slow component initial conditions) and Type II (long-term forced warming trend).*

---

## Delta-Change and Bias-Adjustment

Two approaches to converting raw climate model output into usable data:

**Delta-change:** historic observed (or reanalysis) data is adjusted to account for a future change simulated by a climate model. The change signal from the model is applied on top of real historical data.

*Key advantage over bias-adjustment:* the resulting dataset is built on real historical weather, preserving the physical coherence of the original records — the relationships between variables, storm structures, and multi-day sequences are exactly as they occurred. A model simulation produces plausible but synthetic weather that may not replicate these structures with the same fidelity.

**Bias-adjustment:** model output is corrected to better match observed historical statistics. Can produce longer datasets useful for sampling rare extreme events, but the underlying data is synthetic.
