# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# <img src="https://feature.express/img/logo_blocky.png" width="100%"/>
#
#

# # Modeling timeseries data
#
# This is a basic example how to convert the data into events and create a declaration of features

# ## Importing Libraries
#
# Here, we are importing pandas for data manipulation and `fexpress` which refers to our custom Rust-based feature engineering library.

import os

import pandas as pd

# ## Loading Data
# We are loading rows from the "weatherAUS.csv" dataset and viewing its columns.

if os.path.exists("/kaggle/input/weather-dataset-rattle-package/weatherAUS.csv"):
    os.system("pip install fexpress")
    df = pd.read_csv(
        "/kaggle/input/weather-dataset-rattle-package/weathert sAUS.csv", nrows=10000
    )
else:
    df = pd.read_csv("datasets/weatherAUS.csv", nrows=100000)
df.head()
import fexpress as fx

# ## Creating Events
# We're iterating through the dataframe and creating a new event for each row, encapsulating various weather attributes. These events are added to the FeatureExpress context.

event_context = fx.FeatureExpress()
for row in df.itertuples():
    event = fx.Event(
        event_id=str(row.Index),
        entities={"city": row.Location},
        event_type="reading",
        event_time=str(row.Date),
        attrs={
            col: row.__getattribute__(col)
            for col in [
                "MinTemp",
                "MaxTemp",
                "Rainfall",
                "Evaporation",
                "Sunshine",
                "WindGustDir",
                "WindGustSpeed",
                "WindDir9am",
                "WindDir3pm",
                "WindSpeed9am",
                "WindSpeed3pm",
                "Humidity9am",
                "Humidity3pm",
                "Pressure9am",
                "Pressure3pm",
                "Cloud9am",
                "Cloud3pm",
                "Temp9am",
                "Temp3pm",
                "RainToday",
            ]
            if row.__getattribute__(col) == row.__getattribute__(col)
        },
    )
    event_context.new_event(event)

# ## Configuration
# Setting up the observation dates and query configurations.

obs_dates_config = fx.ObservationDateConfig(
    interval=fx.sdk.observation_dates_config.Interval(
        date_part=fx.sdk.observation_dates_config.DatePart.DAY,
        entity_types=["city"],
        nth=7,
    )
)
event_scope_config = fx.sdk.event_scope_config.EventScopeConfigClass(
    related_entities_events=["city"]
)
query_config = fx.sdk.query_config.QueryConfig(
    include_events_on_obs_date=False, parallel=False
)

# ## Schema Information
# Print the schema information for the reading event.

print(event_context.event_context.schema())

# ## Querying Features
# Using our defined configuration, we query features like average temperature, wind speed, rainfall, etc. over different time windows.

# %%time
features = event_context.query(
    obs_dates_config=obs_dates_config,
    event_scope_config=fx.sdk.event_scope_config.EventScopeConfigClass(
        related_entities_events=["city"]
    ),
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
        "min(Temp9am) over last 7 days",
    ],
)
print(features.head())
print(features.shape)
