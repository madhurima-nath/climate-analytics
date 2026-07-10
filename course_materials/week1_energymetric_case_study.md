# Week 1 Case Study: EnergyMetric
## Climate Intelligence — University of Reading (FutureLearn)

Source: Audio interview with Maria Noguer, Institute for Environmental Analytics, University of Reading.  
EnergyMetric is a platform developed from a UK Space Agency-funded project to support renewable energy planning.

---

### What types of weather data are of interest to EnergyMetric's clients?

Four variables drive renewable energy generation:

- **Wind speed**: for wind power production
- **Incoming short-wave radiation**: sunlight, for solar production
- **Temperature**: affects both how much energy equipment produces and how much people consume
- **Wave amplitude**: for wave energy; EnergyMetric can handle it, but it is not as central to what they do as wind and solar

On top of these, fire weather matters for infrastructure risk: high winds, high temperatures, low humidity, and low rainfall together create conditions that amplify wildfires. Clients in South America are facing this directly: wildfires and landslides are damaging physical energy assets.

---

### Which aspects of the changing climate are most relevant to EnergyMetric's clients?

Two separate problems, both driven by climate change:

**Physical damage to infrastructure.** Extreme events, including landslides and wildfires, are getting worse in parts of South America. These destroy or damage the assets that generate and distribute electricity.

**Rising electricity demand.** Demand is going up for two reasons. Population growth accounts for some of it. But a warmer climate independently drives more demand: people need more cooling, and drier conditions mean more irrigation for agriculture, which is energy-intensive. Climate change is both damaging the assets and increasing what those assets need to deliver.

One point Maria Noguer makes that is easy to miss: renewable energy is not only a way to cut emissions. It is also an adaptation measure. If the extra demand created by a warmer climate is met with renewables rather than fossil fuels, you reduce emissions and meet the extra demand climate change creates, at the same time.

---

### What timescales are of interest?

Two timescales, serving two different parts of the problem:

**Year-to-year weather variability.** For generation planning, what matters is natural variation in weather: a windier year, a sunnier year, an unusual dry spell. Over the next 10-20 years, this variability has more effect on how much a wind or solar installation produces than long-term climate change does. EnergyMetric builds a 10-year historical weather dataset to capture this range of variability. The same 10-year dataset is used across all future planning scenarios. The 2025 and 2030 scenarios use identical weather inputs; only the demand figures and asset configurations change.

**Decades.** The Colombian companies EnergyMetric works with are planning 10 to 20 years ahead. Demand projections, infrastructure investment, and net-zero targets all sit at this timescale. The Copernicus reanalysis data they start from covers 40-50 years of history, which gives enough depth to understand what the climate at a site has actually looked like.

---

### Is the information highly localised, or is a broader picture sufficient?

Highly localised. The global reanalysis data from the Copernicus Climate Data Store has a resolution of 30x30 kilometres. That is not fine enough. Wind behaves differently around mountains, valleys, and coastlines than a 30km grid can show. Cloud formation at a specific site affects solar output in ways that regional data misses.

EnergyMetric takes the global reanalysis data and runs it through a regional weather model built specifically for the area in question, a process called dynamical downscaling. The physics of how wind flows around terrain and how clouds form is modelled explicitly, not approximated statistically. The goal is weather data that accurately represents what is happening at the actual site where a wind turbine or solar array might go. Broad regional trends are not enough for this.

---

### Do clients need detailed quantitative data, or is summarised information sufficient?

Raw numerical data goes in; scenario comparisons come out. Both exist, but they serve different people at different points in the process.

The underlying work requires detailed numerical data: gridded time series of wind speed, radiation, and temperature, fed into models. This dataset can be purchased separately by clients who want to plug it into their own systems.

The platform itself sits on top of that. It converts weather into power and lets planners run scenarios. For example, if a country installs a certain mix of solar and wind at specific locations, can it reach 30% renewable penetration by 2030? The output at that stage is a scenario comparison. A decision-maker can act on it without reading the raw data directly.
