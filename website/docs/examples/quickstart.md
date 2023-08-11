---
sidebar_position: 6
---

# Quickstart

Let's import some classes.

```python
from fexpress import FExpress, Event, ConstantDates, Feature
from datetime import datetime, timedelta
```

Define various weather conditions.

```python
weather_conditions = ['sunny', 'rainy', 'snowy', 'cloudy', 'windy']
```

First, we create a list of events. Each event corresponds to weather conditions for a single day.

```python
events = []
for i in range(10000):
    event_time = datetime(2020, 1, i+1).isoformat()
    event_attrs = {
        "temperature": round(20 + i - 2*(i**2)/10, 1),  # Simulate a temperature fluctuation
        "pressure": round(1000 + i*2, 1),
        "humidity": round(50 - i + 3*(i**2)/10, 1),  # Simulate a humidity fluctuation
        "weather_condition": weather_conditions[i%len(weather_conditions)],  # Cycle through weather conditions
        "wind_speed": round(abs(10 * i * (i-6) / 15), 1),  # Simulate wind speed fluctuation
    }
    events.append(Event(entities={"location": "Warsaw"}, event_type="weather", time=event_time, attrs=event_attrs))
```

We then define two observation dates for which we will calculate the features.

```
obs_dates = ConstantDates(["2020-01-05T00:00:00", "2020-01-07T00:00:00"]).to_obs_dates()
```

Next, we define features to be calculated using the events and observation dates. 
These features include the latest weather conditions and aggregated data over various time periods.

```
features = [
    Feature("@location"),
    Feature("obs_dt"),
    Feature("last(pressure) over past where event_type = 'weather'"),
    Feature("last(temperature) over past where event_type = 'weather'"),
    Feature("last(humidity) over past where event_type = 'weather'"),
    Feature("last(weather_condition) over past where event_type = 'weather'"),
    Feature("last(wind_speed) over past where event_type = 'weather'"),
    Feature("avg(temperature) over last 3 days where event_type = 'weather'"),
    Feature("count(*) over last 5 days where attr(weather_condition) = 'sunny'"),
    Feature("max(pressure) over last 3 days where event_type = 'weather'"),
    Feature("min(humidity) over last 3 days where event_type = 'weather'"),
    Feature("sum(wind_speed) over last 7 days where event_type = 'weather'")
]
```

Initialize the Feature Express instance and feed the events into Feature Express instance.

```python
fexpress = FExpress()
for event in events:
    fexpress.new_event(event)
```

Finally, extract features using the defined events and observation dates.

```python
df = fexpress.extract_features(features, obs_dates)
```

In this code, we have expanded the features to also include average temperature over the past 3 days, count of sunny days in the past 5 days, maximum pressure and minimum humidity over the past 3 days, and the sum of wind speeds over the last 7 days. The specific functions and attributes can of course be adjusted according to the actual needs and the available data.
