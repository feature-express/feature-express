#!/usr/bin/env python
# coding: utf-8

# # Importing Libraries
# Here, we are importing pandas for data manipulation and `fexpress` which refers to our custom Rust-based feature engineering library.
#

# In[1]:

import pandas as pd
import fexpress as fx

# # Loading Data
# We are loading rows from the "weatherAUS.csv" dataset and viewing its columns.

# In[2]:

df = pd.read_csv("datasets/weatherAUS.csv", nrows=1000)
df.head()
df.columns

# # Creating Events
# We're iterating through the dataframe and creating a new event for each row, encapsulating various weather attributes. These events are added to the FeatureExpress context.
#

# In[3]:

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

# # Configuration
# Setting up the observation dates and query configurations.

# In[4]:

obs_dates_config = fx.ObservationDateConfig(interval=fx.sdk.observation_dates_config.Interval(
    date_part=fx.sdk.observation_dates_config.DatePart.DAY,
    entity_types=["city"],
    nth=7
))
event_scope_config=fx.sdk.event_scope_config.EventScopeConfigClass(related_entities_events=["city"])
query_config = fx.sdk.query_config.QueryConfig(include_events_on_obs_date=False, parallel=True)

# # Schema Information
# Print the schema information for the reading event.

# In[8]:

print(event_context.event_context.schema())

# # Querying Features
# Using our defined configuration, we query features like average temperature, wind speed, rainfall, etc. over different time windows.

# In[11]:

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
print(features.head())
