# Week 1 Case Study: Airspace Unlimited
## Climate Intelligence — University of Reading (FutureLearn)

Source: Audio interview with Doug Meyerhoff, co-founder, Airspace Unlimited Scotland.
Airspace Unlimited Scotland optimises military airspace reservations to reduce their impact on civilian aviation, cutting fuel costs and CO2 emissions.

---

### What types of weather data are of interest to your business?

One variable dominates: winds aloft forecasts. These are readings of wind direction and speed at different altitudes across the airspace. Wind behaviour changes with altitude; the jet stream creates the dominant flow pattern across the North Atlantic, and its position determines which routes transatlantic traffic will prefer. A northerly jet stream pulls traffic onto northerly routes; when it shifts, traffic demand moves with it.

Underlying the winds are pressure systems. High- and low-pressure movements drive the air masses that create jet stream behaviour and determine the wind patterns that routing decisions depend on.

Data sources named in the transcript: the UK Met Office, a European ensemble forecast system, and ECMWF (the European Centre for Medium-Range Weather Forecasts). Doug Meyerhoff is clear about source preference: he wants raw data from as close to the original source as possible, fed directly into Airspace Unlimited's internal algorithm.

---

### Which aspects of the changing climate are most relevant?

Airspace Unlimited differs from the other two Week 1 case studies here. The transcript does not discuss climate projections, long-term trends, or how a changing climate affects their operational needs. Their work is built entirely on short-term weather forecasting.

The climate connection enters differently: reducing CO2 emissions from aviation is the stated motivation behind the service. More direct flight routes mean less fuel burned, which means fewer emissions. Doug Meyerhoff describes this as the "underlying current" running through their commercial offering.

He also notes a shift in emphasis. When the interview was recorded, events in Ukraine had pushed fuel cost and fuel availability to the front. The commercial case for the service had sharpened, even as the environmental motivation remained. The two goals are aligned: efficient routing reduces cost and emissions together. But the urgency had changed.

---

### What timescales are of interest? Do you need the outlook for the coming season or are you looking for information on the longer term impacts of global warming – to 2050 or perhaps even 2100?

Short-term and operational only. The critical window is 12 to 72 hours.

Airlines in Europe (easyJet, Ryanair, and British Airways are named) block-book their flight plans 12 hours in advance. Once those plans are filed, operators are locked in; they cannot easily reroute even if a more efficient option becomes available within that window. Airspace Unlimited needs to get reservations optimised before the 12-hour filing point. Reliable winds-aloft forecasts out to 72 hours provide enough lead time to do that.

Nothing in the transcript points to seasonal, annual, or multi-decade timescales. The course asks whether information is needed to 2050 or 2100; for Airspace Unlimited, that question does not apply. This is a day-to-day operational service, not a strategic planning tool built on climate projections.

---

### Do you need detailed information about a particular place or time, or is a broader picture of large scale geographical trends sufficient?

Both precision and scale matter, for different parts of the problem.

At the large scale, the question is regional: where is the jet stream, how will it move over the next 72 hours, and which flight corridors across the North Atlantic will be preferred as a result? That requires a picture of wind patterns across a wide area.

At the local level, the output is precise: the shape and orientation of an individual airspace reservation, defined by coordinates, at a specific location. A north-south rectangle blocks far more civilian traffic than an east-west one covering the same area. The spatial detail in the wind forecast has to be good enough to inform that geometry. Doug Meyerhoff describes shaving corners off crude rectangular reservations; that level of precision requires knowing what the wind is actually doing at cruise altitude in the specific airspace involved.

---

### Do you need access to detailed quantitative numerical data to feed into in-house models of your business activities or assets, or is summarised climate information sufficient (eg, a graph or a plot)?

Raw quantitative data, fed directly into their algorithm. Doug Meyerhoff is direct: "We want to get the data from as close to the original source as possible. That gives us the ability to then feed that information directly into our algorithm."

The pattern is the same as EnergyMetric: the company ingests raw numerical data, processes it internally, and produces outputs that clients can act on. Military mission planners receive shaped reservation recommendations. Airlines get efficient airspace without having to reverse-engineer their flight plans. The system does the computation; the clients receive the results.

The contrast with Historic England is consistent across both cases. Historic England's clients need accessible summaries because the challenge is awareness and prioritisation. Airspace Unlimited's clients need decisions made for them, on the basis of data they never need to see directly.

---

### What would this data, graph or modelling process look like?

The transcript describes the process at a high level:

**Input:** winds-aloft forecasts from the UK Met Office, European ensemble systems, and ECMWF. Raw data is taken from as close to the original source as possible.

**Processing:** the raw data feeds directly into Airspace Unlimited's algorithm, which produces what Doug Meyerhoff calls "the most accurate forecasts and track prediction." The algorithm works out where civilian traffic will want to be given the wind, and from that determines how military reservations should be shaped and positioned.

**Output:** shaped airspace reservations. Rather than crude rectangular blocks, corners are shaved and dimensions adjusted to give the military exactly what they need while opening as much airspace as possible to civilian traffic. The transcript does not describe the visual format of the output.

Doug Meyerhoff also describes extending the same routing algorithm to ships: reducing track mileage and improving fuel burn predictability for maritime operators.
