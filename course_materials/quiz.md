# Course Quiz: Questions and Answers
## Climate Intelligence — University of Reading (FutureLearn)

---

### Question 1

**Early in Week 2 we discussed some of the different forms of climate data and highlighted 'reanalysis' datasets as a particularly important type. What is a reanalysis?**

- A set of surface weather-analysis maps derived from observations collected over many decades.
- **A gridded 3-D reconstruction of the atmospheric circulation over many decades, blending observational and model simulation data using 'data assimilation'.** ✓
- A gridded climate model reconstruction of the weather over the last several decades, driven by observed historic levels of greenhouse gas concentrations.
- A revised numerical weather forecast, produced with the benefit of hindsight.

**Explanation:** The course defines reanalyses as "comprehensive, 3-dimensional, gridded, historic, weather datasets...combining state-of-the-art numerical models with historical observations through data assimilation." The other options are wrong because: surface maps only misses the 3-D character; GHG-driven model reconstruction describes a Type II climate projection, not a reanalysis; and "revised forecast with hindsight" misrepresents the stepwise blending process that runs continuously forward in time.

---

### Question 2

**Climate data takes many forms and often we may be forced to choose which data are most suitable. Which of the following would you regard as the 'best' quality data?**

- Site-based meteorological estimates (eg, wind speeds measured at a site).
- Remote sensing data from a satellite (eg, an estimate of surface temperature pixel).
- Reanalysis (eg, an estimate of gridded surface weather).
- **None or all of the above — data quality depends on what it is being used for.** ✓

**Explanation:** The course is explicit: "no single data source is perfect as they all have their advantages and limitations. It's vital to assess which best meets your needs and if it's 'fit for purpose'." Site-based data is best where well-sited instruments exist. Satellite data gives global coverage but depends on calibration assumptions. Reanalysis provides consistent 3-D historical fields but is limited by resolution and can carry spurious trends. "Best" is not a property of the data type — it is a property of the match between the data and the specific decision.

---

### Question 3

**Which of the following would usually be included in a climate model (GCM) but not in a numerical weather prediction (NWP) model?**

- Equations of motion (based on a version of Newton's laws).
- **A detailed physical representation of slow evolving components such as the ocean and sea-ice.** ✓
- Physical 'parameterisation schemes' representing processes such as surface momentum exchange, radiation and clouds.
- A physical representation of the land surface.

**Explanation:** NWP models are built for initial condition predictability over days. The ocean and sea-ice evolve on timescales of weeks to decades — far beyond the NWP window — so including them adds computational cost with no practical benefit. GCMs need them because boundary condition projections over decades are driven by these slowly evolving components. The other three (equations of motion, parameterisation schemes, land surface) are present in both model types: they are fundamental to simulating the atmosphere at any timescale.

---

### Question 4

**What kind of predictability is typically being used in weather forecasts a few days ahead?**

- **Initial condition (Type I) — the predictability comes from knowing the starting state accurately.** ✓
- Boundary condition (Type II) — the predictability comes from being able to detect a 'change' produced by some externally applied forcing.
- Mixed (Type I and II) — the predictability comes from a mixture of initial conditions and boundary conditions.
- Climatology (Type 0) — the predictability relates to accurately knowing how the property varies in the absence of any external forcing.

**Explanation:** A days-ahead weather forecast depends entirely on knowing the current state of the atmosphere accurately and tracking how it evolves. The pegboard analogy: knowing where the ball is dropped (the initial condition) determines which bucket it falls into. The Butterfly Effect sets a hard physical limit on this type of predictability at approximately several days, regardless of how accurately the starting state is known.

---

### Question 5

**What kind of predictability is typically being used in estimating the change in rainfall at a site that might be expected under a 2100 high greenhouse gas emissions scenario?**

- Mixed (Type I and II) — the predictability comes from a mixture of initial conditions and boundary conditions.
- Initial condition (Type I) — the predictability comes from knowing the starting state accurately.
- **Boundary condition (Type II) — the predictability comes from being able to detect a 'change' produced by some externally applied forcing.** ✓
- Climatology (Type 0) — the predictability relates to accurately knowing how the property varies in the absence of any external forcing.

**Explanation:** A 2100 projection is decades ahead — far beyond any initial condition predictability. What is being estimated is not a specific weather event but how the overall distribution of rainfall shifts in response to increased greenhouse gas concentrations. The pegboard analogy: the concern is not which bucket any individual ball falls into, but how tipping the board (the greenhouse gas forcing) changes the overall pattern of where balls land. The current atmospheric state is irrelevant to a 2100 projection.

---

### Question 6

**What type of prediction is typically being used in a forecast of the European area-average surface temperature for the season ahead?**

- **Mixed (Type I and II) — the predictability comes from a mixture of initial conditions and boundary conditions.** ✓
- Boundary condition (Type II) — the predictability comes from being able to detect a 'change' produced by some externally applied forcing.
- Climatology (Type 0) — the predictability relates to accurately knowing how the property varies in the absence of any external forcing.
- Initial condition (Type I) — the predictability comes from knowing the starting state accurately.

**Explanation:** A season ahead sits in the S2S middle ground. Fast atmospheric initial condition predictability is lost after a few days. But slowly evolving components — ocean temperatures, land surface state, upper atmosphere — retain initial condition information over weeks to months, and those slow components act as boundary conditions on the faster atmosphere. The course notes: "boundary condition predictability can be derived from the initial conditions of more slowly evolving components of the climate system." A seasonal forecast also carries a Type II signal: the long-term forced warming trend shifts the baseline upward. Both contribute.

---

### Question 7

**What type of predictability is typically being used by an insurance company estimating the surface wind speeds associated with a 1-in-10 year storm using historical data from the last 40 years?**

- Mixed (Type I and II) — the predictability comes from a mixture of initial conditions and boundary conditions.
- Initial condition (Type I) — the predictability comes from knowing the starting state accurately.
- Boundary condition (Type II) — the predictability comes from being able to detect a 'change' produced by some externally applied forcing.
- **Climatology (Type 0) — the predictability relates to accurately knowing how the property varies in the absence of any external forcing.** ✓

**Explanation:** The insurance company is not forecasting a specific storm (Type I) and is not projecting how a forcing will shift the distribution (Type II). It is using 40 years of historical data to characterise the statistical properties of the climate as it has been. A 1-in-10 year return period is a statistical property of that historical distribution. This is exactly the Type 0 definition: "derived from the statistical properties of a system where there are no changes to the boundary conditions." The key limitation: if the climate is changing, a historical sample may be an unreliable basis for estimating future return periods.

---

### Question 8

**Delta-change refers to the process by which historic observed (or reanalysis) data is adjusted to account for a future change in climate simulated by a climate model. A key advantage of this method over bias-adjustment is:**

- **The resulting dataset corresponds to 'real' historic weather and therefore has a high level of fidelity in terms of the meteorological structure and process.** ✓
- The resulting dataset is much longer than the historic observation allowing you to better sample rare extreme events.
- The resulting dataset relies on 'real' historic weather and is therefore a more trustworthy guide to the weather events that will be observed in the future than those produced by a climate model.
- It is faster and simpler to code.

**Explanation:** Delta-change starts from real observed or reanalysis data and applies a modelled change signal on top. Because the underlying sequences are real weather, physical coherence is preserved: relationships between variables, storm structures, and multi-day sequences are exactly as they occurred. A model simulation produces plausible but synthetic weather that may not replicate those structures with the same fidelity. The other options do not describe advantages of delta-change specifically: longer datasets apply to model-based approaches; physical realism does not mean future events will recur identically; simplicity of coding is not a quality advantage.

---

### Question 9

**Designing and constructing models to analyse the impacts of climate on business activities can be an extremely challenging process. Which of the following approaches to applying them is not worthwhile?**

- Clearly identifying the purpose of the modelling and specifying what the model needs to be able to 'do'.
- Listing (as far as is possible) the uncertainties and assumptions made in the modelling process and identifying whether the model is 'fit for purpose'.
- Engaging in understanding both the data being used and the decisions being made.
- **Sensitivity testing every assumption made in the modelling process.** ✓

**Explanation:** The other three approaches are all explicitly endorsed by the course as essential. Sensitivity testing is not inherently bad practice, but testing *every* assumption is neither practical nor necessary. Climate models contain countless assumptions — from grid box size to parameterisation choices to emissions pathways. Some are well-established physical principles that do not need testing. Some are "difficult or even impossible to fully evaluate." The course approach is targeted: identify which uncertainties matter most for the specific decision and focus scrutiny there, not attempt exhaustive testing of everything.
