# Week 1 Course Notes: How Climate Data is Used in Business
## Climate Intelligence — University of Reading (FutureLearn)

---

## Why Climate Intelligence?

Climate-related disasters jumped 83% in the past 20 years. Yet 76% of global CEOs stated they are under-prepared for climate change. Understanding climate risk is no longer optional: UK legislation now requires large businesses to report climate-related risk in line with TCFD recommendations, and central banks in the UK, France, Singapore and the Netherlands have all suggested climate stress tests for investments.

### Risk Taxonomy

Climate risks fall into three categories:

**Physical risks** are the most visible. Key indicators include heat stress, water stress, extreme precipitation, wildfires, sea level rise, and hurricanes and typhoons. These can be acute (sudden events) or chronic (gradual trends).

**Transition risks** arise from the measures taken to adapt to or mitigate climate change:

- *Adaptation risks*: negative operational impacts, costs of adjusting the business, asset valuation changes, subsidy loss. Risk profiles change as adaptation measures are implemented, requiring ongoing review.
- *Mitigation risks*: impacts from activities directed at reducing climate change — changes in regulation, legislation, technology, and critical infrastructure.

**Natural capital risks** are less well understood but growing in importance. Natural capital refers to the global stock of natural resources: soil, clean air, groundwater, biodiversity. Moody's has begun incorporating natural capital into its ESG assessments, with five exposure categories: carbon transition, physical climate risks, water management, waste and pollution, and natural capital.

### Risk Reporting

The IPCC has emphasised that risks apply to both the impacts of and the responses to climate change. The Task Force on Climate-related Financial Disclosures (TCFD) is pushing organisations to disclose their climate risk assessments. In January 2020, BlackRock CEO Larry Fink asked companies in which they invest to disclose climate-related risks in line with TCFD recommendations and to set science-based emissions reduction targets.

---

## Using Climate Intelligence for Planning

Climate change impacts — rising sea levels, flooding, drought, crop failure, heat stress — become more frequent as temperatures increase. For many organisations, the most significant effects will emerge over the medium to longer term. The actions governments will take remain uncertain, which makes planning challenging.

### TCFD Scenario Analysis

The TCFD recommends that organisations analyse their resilience to climate risk under several different climate scenarios. These scenarios assume a particular temperature rise (1.5°C, 2.1°C, 3°C, etc.) and provide frameworks for forecasting the climate-related impacts each might bring. They are not intended to deliver precise outcomes but to provide a structured way to consider how the future might look.

Organisations are asked to:
- Carry out scenario analysis
- Disclose their assessment publicly so investors and stakeholders can understand vulnerability
- Review and rework the analyses regularly as climate impacts become clearer

---

## Case Study 1: EnergyMetric

### Interview context

Maria Noguer (Institute for Environmental Analytics, University of Reading) describes EnergyMetric — a platform developed from a UK Space Agency-funded project to support renewable energy planning. It helps stakeholders in Colombia making decisions about the transition to low-carbon energy systems over a 10-20 year planning horizon.

### How EnergyMetric converts data to intelligence

Three core challenges drive the need for climate intelligence in the energy sector:

- Extreme weather hazards (flooding, wildfires) damaging infrastructure and assets
- Increasing weather-sensitive energy demand (cooling, irrigation)
- Increasing use of weather-sensitive generation (wind, solar)

**Data used:** reanalysis data from the Copernicus Climate Data Store, starting at approximately 30km grid resolution, covering around 40-50 years of history. EnergyMetric uses a 10-year sample for their product.

**Process:** three stages — reanalysis as the base, dynamical downscaling using a regional weather model (to get from 30km to site-level resolution), and an impact model that converts raw meteorological variables (wind speed, solar radiation, temperature) into energy outputs (wind power, solar power, demand figures).

**Type of predictability used:** Type 0 — climatological. The same 10-year historical dataset is applied across all future planning scenarios. At 10-20 year horizons, the long-term climate change signal is relatively modest compared to natural year-to-year variability, making historical climatology a reasonable basis.

**Key insight from the course notes:** "Having detailed spatial information is particularly important — users wish to know the weather and climate at particular sites." The 30km reanalysis grid is not fine enough; downscaling is essential.

---

## Case Study 2: Historic England

### Interview context

Dr Hannah Fluck (Head of Environmental Strategy, Historic England) describes how Historic England uses long-term climate projections to help the heritage sector — archaeological sites, buildings, parks and gardens, coastal wrecks, designed landscapes — plan for a changing climate.

### How Historic England uses long-term projections to plan current priorities

Historic England faces challenges managing assets that have existed for centuries and must be preserved for centuries more. Hannah emphasises that the most dramatic weather hazards are not necessarily the greatest risk: overheating turns out to be more significant than storms or flooding when assets are mapped against hazard data.

**Data used:** UK Climate Projections (UKCP), already downscaled from global climate model outputs to a 5km grid, combined with publicly available data from the Met Office, BGS, Ordnance Survey, and Environment Agency.

**Type of predictability used:** Type II — boundary condition. Long-term projections based on high emissions scenarios showing how the forced shift in climate hazards will affect the distribution of risk over decades to 2060 and beyond.

**Key insight:** Despite the coarse resolution, the course notes observe that "perhaps because Historic England is typically thinking about a much longer timescale than the clients of EnergyMetric, Hannah still finds that useful intelligence is available from these long-term climate projections." Identifying 'no regret' decisions and broad trends is enough to act on, knowing plans can be updated as the climate unfolds.

---

## Case Study 3: Airspace Unlimited

### Interview context

Doug Meyerhoff (co-founder, Airspace Unlimited Scotland) describes how numerical weather forecast data — specifically winds-aloft forecasts — is used to optimise military airspace reservations, reducing their impact on civilian aviation and cutting fuel costs and CO2 emissions.

### How businesses use forecasts to operate more efficiently in a variable climate

Historically, routing decisions in aviation have been made using worst-case weather scenarios, which tend to be operationally restrictive. Advanced forecasting now offers greater accuracy for flight management, allowing more flexibility in routing and scheduling for both military and commercial operations.

**Data used:** numerical weather forecast data — winds-aloft forecasts from the UK Met Office, a European ensemble forecast system, and ECMWF. Raw data fed directly into a proprietary routing algorithm.

**Type of predictability used:** Type I — initial condition. The 72-hour forecast window is right at the limit of useful atmospheric initial condition predictability. The Butterfly Effect sets a hard limit of approximately several days.

**Extended-range forecasts (S2S):** The course introduces sub-seasonal to seasonal (S2S) forecasting as an emerging development beyond this window. Once atmospheric initial condition predictability is lost, skill over weeks to months can be derived from the initial conditions of more slowly evolving components: oceans, land surface, upper atmosphere. These slow components act as boundary conditions on the faster atmosphere. S2S forecasts are probabilistic and relate to distributions over larger areas and longer periods, not specific events. The predictability type is a mixture of Type I (from slow components) and Type II (long-term forced warming trend).

The predictive skill of numerical weather forecasts has increased at approximately one day per decade. A 5-day lead time forecast in 2020 is about as reliable as a 4-day forecast in 2010. This improvement cannot continue indefinitely — the Butterfly Effect sets a fundamental physical limit.
