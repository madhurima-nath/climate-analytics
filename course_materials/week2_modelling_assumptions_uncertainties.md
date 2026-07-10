# Week 2: Modelling Assumptions, Uncertainties and Limitations
## Climate Intelligence — University of Reading (FutureLearn)

Source: Course notes — Introduction to climate data, Principles of climate models, Types of predictability.

---

### What assumptions are being made in this modelling process?

**Grid box homogeneity.** Climate models divide the Earth into grid boxes, typically around 100 sq km, and treat conditions within each box as uniform. Real landscapes — hills, coastlines, cities — create local variations a single box value cannot represent.

**Parameterisation of sub-grid processes.** Physical processes too small to resolve at grid scale (convection, turbulence, cloud formation) are approximated mathematically rather than modelled from first principles. The approximation introduces choices that affect the output.

**Reanalysis as best estimate.** Reanalyses blend numerical weather prediction forecasts with historical observations through data assimilation. The course describes the result as a "best estimate" of past atmospheric state — both the forecast and the observations are imperfect, and the blend is assumed to be better than either alone.

**Historical climate as a proxy for future conditions (Type 0).** For near-term planning horizons of 10-20 years, historical climate data is assumed to be a reasonable characterisation of future weather variability. EnergyMetric applies this directly: the same 10-year historical dataset is used across all future scenarios.

**Emissions pathway assumptions (Type II).** Longer-term projections assume future greenhouse gas concentrations follow a defined scenario. The course notes these scenarios "are not intended to deliver precise outcomes or forecasts, but to provide a way for organisations to consider how the future might look."

**Observational network adequacy.** Data assimilation assumes the observational network is sufficiently dense and consistent to constrain reanalysis output reliably across the globe and across time.

---

### List some of the uncertainties and limitations these assumptions lead to.

**Resolution.** The grid box size means highly localised meteorological conditions cannot be directly resolved. The course illustrates this with the wind farm on a hill: nearby masts on the plains give poor estimates of wind over the hill; a 30km reanalysis grid box fails entirely. Downscaling reduces but does not eliminate this problem.

**Model biases and deficiencies.** Numerical models have errors in their representation of physical processes. The course states these are "difficult or even impossible to fully evaluate." Surface properties such as precipitation are particularly prone to bias.

**Spurious trends in reanalyses.** Changes in the observational network over time — new satellite systems, new instrument types — can introduce apparent trends that reflect changes in the data rather than real changes in the climate.

**Natural decadal variability obscuring forced signals.** The climate system contains slowly varying components with multi-decade timescales. The course illustrates this with a diagram showing how a 30-year natural cycle can make a genuine long-term warming trend appear as a cooling trend over any 25-year sample. A short data record can give a deeply misleading picture of the underlying forced signal.

**Future emissions uncertainty.** Which emissions pathway will actually unfold is unknown. Scenario-based projections explore a range of possibilities but cannot determine which will occur.

---

### Which uncertainties are epistemic and which aleatory in nature?

**Epistemic uncertainties** arise from incomplete knowledge and are reducible in principle:

- **Model biases and parameterisation choices.** Better understanding of physical processes and higher-resolution models can reduce these, though not eliminate them entirely.
- **Grid resolution limitations.** Higher resolution reduces spatial uncertainty, at increased computational cost.
- **Calibration assumptions in remote sensing.** Better calibration models and more extensive validation data help.
- **Changes and gaps in observational networks.** Denser, more consistent observational networks reduce the spurious trends that enter reanalyses.
- **Future emissions pathway.** Better knowledge of policy trajectories and technological change would narrow scenario uncertainty.

**Aleatory uncertainties** are inherent to the system and cannot be reduced by more knowledge or better models:

- **Natural variability of the climate system.** Year-to-year and decadal-scale fluctuations are real features of the climate, not modelling errors. A 30-year slow cycle will always create a signal that can mask a forced trend over shorter samples.
- **Chaotic behaviour of the atmosphere.** The Butterfly Effect sets a hard physical limit on initial condition predictability at around several days. This is a fundamental property of atmospheric dynamics, not a gap in current models.
- **Unpredictability of individual weather events beyond that limit.** Even a perfect model cannot predict a specific weather event two weeks ahead. What becomes predictable is the distribution of outcomes, not the individual event.

---

### How might they affect your interpretation of the model's output?

**Aleatory uncertainty requires accepting irreducible limits:**

Short data samples can be deeply misleading. The course is explicit: where slow natural variability exists, a few years of data may produce an estimate "much warmer or colder than the true long-term average." Several decades are needed to identify the forced signal reliably.

Single-value (deterministic) forecasts misrepresent what the models actually know. Probabilistic outputs that express a range of likely outcomes are more appropriate and more honest about what predictions can and cannot tell you.

For decisions with irreversible consequences, worst-case scenarios are more defensible than most-likely ones. Historic England's approach — using high emissions projections for irreplaceable heritage assets — is the course's own illustration of this reasoning applied in practice.

**Epistemic uncertainty requires caution about what the data is actually showing:**

Model output can actively mislead if its biases are not understood before use. The course warns directly: "climate data can be quite misleading if it's not handled appropriately."

Coarse-resolution data should not be applied to site-specific decisions without downscaling. A national hazard map identifies priorities across a large area; it cannot drive decisions about an individual asset or site without further work to resolve local conditions.

Ensemble approaches — running multiple models or multiple realisations of the same model — reduce but do not eliminate the risk of a single model's particular biases driving the conclusion. Treating any single model run as the answer is a misuse of what the modelling process can provide.
