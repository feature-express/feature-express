---
sidebar_position: 7
---

# Modeling weather data

# Importing Libraries
Here, we are importing pandas for data manipulation and `fexpress` which refers to our custom Rust-based feature engineering library.

```python
import pandas as pd
import fexpress as fx
```

# Loading Data
We are loading rows from the "weatherAUS.csv" dataset and viewing its columns.


```python
df = pd.read_csv("datasets/weatherAUS.csv")
df.head()
df.columns
```

    Index(['Date', 'Location', 'MinTemp', 'MaxTemp', 'Rainfall', 'Evaporation',
           'Sunshine', 'WindGustDir', 'WindGustSpeed', 'WindDir9am', 'WindDir3pm',
           'WindSpeed9am', 'WindSpeed3pm', 'Humidity9am', 'Humidity3pm',
           'Pressure9am', 'Pressure3pm', 'Cloud9am', 'Cloud3pm', 'Temp9am',
           'Temp3pm', 'RainToday', 'RainTomorrow'],
          dtype='object')

# Creating Events
We're iterating through the dataframe and creating a new event for each row, encapsulating various weather attributes. These events are added to the FeatureExpress context.

```python
event_context = fx.FeatureExpress()
for row in df.itertuples():
    event = fx.Event(
        event_id=str(row.Index),
        entities={"city": row.Location},
        event_type="reading",
        event_time=str(row.Date),
        attrs={col: row.__getattribute__(col) for col in ['MinTemp', 'MaxTemp', 'Rainfall', 'Evaporation',
       'Sunshine', 'WindGustDir', 'WindGustSpeed', 'WindDir9am', 'WindDir3pm',
       'WindSpeed9am', 'WindSpeed3pm', 'Humidity9am', 'Humidity3pm',
       'Pressure9am', 'Pressure3pm', 'Cloud9am', 'Cloud3pm', 'Temp9am',
       'Temp3pm', 'RainToday'] if row.__getattribute__(col)==row.__getattribute__(col)},
    )
    event_context.new_event(event)
```

# Configuration
Setting up the observation dates and query configurations.

```python
obs_dates_config = fx.ObservationDateConfig(interval=fx.sdk.observation_dates_config.Interval(
    date_part=fx.sdk.observation_dates_config.DatePart.DAY,
    entity_types=["city"],
    nth=7
))
event_scope_config=fx.sdk.event_scope_config.EventScopeConfigClass(related_entities_events=["city"])
query_config = fx.sdk.query_config.QueryConfig(include_events_on_obs_date=False, parallel=True)
```

# Schema Information
Print the schema information for the reading event.

```python
print(event_context.event_context.schema())
```

    {
      "reading": {
        "RainToday": "Str",
        "Cloud3pm": "Num",
        "WindGustSpeed": "Num",
        "Humidity3pm": "Num",
        "Sunshine": "Num",
        "Humidity9am": "Num",
        "WindDir9am": "Str",
        "MaxTemp": "Num",
        "WindSpeed9am": "Num",
        "Temp9am": "Num",
        "Pressure3pm": "Num",
        "Evaporation": "Num",
        "Pressure9am": "Num",
        "WindDir3pm": "Str",
        "Rainfall": "Num",
        "Cloud9am": "Num",
        "MinTemp": "Num",
        "WindGustDir": "Str",
        "WindSpeed3pm": "Num",
        "Temp3pm": "Num"
      }
    }


# Querying Features
Using our defined configuration, we query features like average temperature, wind speed, rainfall, etc. over different time windows.


```python
features = event_context.query(
    obs_dates_config=obs_dates_config,
    event_scope_config=fx.sdk.event_scope_config.EventScopeConfigClass(related_entities_events=["city"]),
    query_config=query_config,
    query=[
        "obs_dt as obs_dt",
        "@entities.city as city",
        "avg(MaxTemp) over last 7 days",
        "min(MinTemp) over last 7 days",
        "max(WindGustSpeed) over last 3 days",
        "last(Humidity3pm) over past",
        "first(Humidity9am) over future",
        "sum(Rainfall) over last 30 days",
        "avg(WindSpeed9am) over last 5 days",
        "avg(WindSpeed3pm) over last 5 days",
        "last(Temp3pm) over last 3 days",
        "first(Temp9am) over last 3 days",
        "count(*) over last 30 days where RainToday = 'Yes' as rainy_days",
        "count(*) over last 30 days where RainToday = 'No' as non_rainy_days",
        "avg(Cloud9am) over last 7 days",
        "avg(Cloud3pm) over last 7 days",
        "sum(Pressure9am) over last 3 days",
        "sum(Pressure3pm) over last 3 days",
        "last(WindGustDir) over past",
        "max(Temp3pm) over last 7 days",
        "min(Temp9am) over last 7 days"
    ]
)
features.head()
```
