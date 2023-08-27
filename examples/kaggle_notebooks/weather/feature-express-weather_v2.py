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
import time

# ## Loading Data
# We are loading rows from the "weatherAUS.csv" dataset and viewing its columns.

if os.path.exists("/kaggle/input/weather-dataset-rattle-package/weatherAUS.csv"):
    os.system("pip install fexpress")
    df = pd.read_csv(
        "/kaggle/input/weather-dataset-rattle-package/weathert sAUS.csv"
    )
else:
    df = pd.read_csv("datasets/weatherAUS.csv")
df.head()
import fexpress as fx

# Pandas
window = 180
start_time = time.time()
def calculate_features(city_df):
    features = {
        "avg(MaxTemp) over last 7 days": city_df['MaxTemp'].rolling(window=window).mean(),
        "min(MinTemp) over last 7 days": city_df['MinTemp'].rolling(window=window).min(),
        "max(WindGustSpeed) over last 3 days": city_df['WindGustSpeed'].rolling(window=window).max(),
        "sum(Rainfall) over last 30 days": city_df['Rainfall'].rolling(window=window).sum(),
        "avg(WindSpeed9am) over last 5 days": city_df['WindSpeed9am'].rolling(window=window).mean(),
        "avg(WindSpeed3pm) over last 5 days": city_df['WindSpeed3pm'].rolling(window=window).mean(),
        "rainy_days": (city_df['RainToday'] == 'Yes').rolling(window=window).sum(),
        "non_rainy_days": (city_df['RainToday'] == 'No').rolling(window=window).sum(),
        "avg(Cloud9am) over last 7 days": city_df['Cloud9am'].rolling(window=window).mean(),
        "avg(Cloud3pm) over last 7 days": city_df['Cloud3pm'].rolling(window=window).mean(),
        "sum(Pressure9am) over last 3 days": city_df['Pressure9am'].rolling(window=window).sum(),
        "sum(Pressure3pm) over last 3 days": city_df['Pressure3pm'].rolling(window=window).sum(),
        "max(Temp3pm) over last 7 days": city_df['Temp3pm'].rolling(window=window).max(),
        "min(Temp9am) over last 7 days": city_df['Temp9am'].rolling(window=window).min(),
    }
    return pd.DataFrame(features)

feature_frames = []
for city in df['Location'].unique():
    city_df = df[df['Location'] == city]
    features = calculate_features(city_df)
    features['Location'] = city
    features['ObservationDate'] = city_df['Date']
    feature_frames.append(features)

final_features_df = pd.concat(feature_frames).reset_index(drop=True)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Pandas elapsed time: {elapsed_time} seconds")

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
        nth=1,
    )
)
event_scope_config = fx.sdk.event_scope_config.EventScopeConfigClass(
    related_entities_events=["city"]
)
query_config = fx.sdk.query_config.QueryConfig(
    include_events_on_obs_date=False, parallel=True
)

# ## Schema Information
# Print the schema information for the reading event.

print(event_context.event_context.schema())

# ## Querying Features
# Using our defined configuration, we query features like average temperature, wind speed, rainfall, etc. over different time windows.

# %%time
start_time = time.time()
features = event_context.query(
    obs_dates_config=obs_dates_config,
    event_scope_config=fx.sdk.event_scope_config.EventScopeConfigClass(related_entities_events=["city"]),
    query_config=query_config,
    query=[
        f"obs_dt as obs_dt",
        f"@entities.city as city",
        f"avg(MaxTemp) over last {window} days",
        f"min(MinTemp) over last {window} days",
        f"max(WindGustSpeed) over last {window} days",
        f"sum(Rainfall) over last {window} days",
        f"avg(WindSpeed9am) over last {window} days",
        f"avg(WindSpeed3pm) over last {window} days",
        f"last(Temp3pm) over last {window} days",
        f"count(*) over last {window} days where RainToday = 'Yes' as rainy_days",
        f"count(*) over last {window} days where RainToday = 'No' as non_rainy_days",
        f"avg(Cloud9am) over last {window} days",
        f"avg(Cloud3pm) over last {window} days",
        f"sum(Pressure9am) over last {window} days",
        f"sum(Pressure3pm) over last {window} days",
        f"max(Temp3pm) over last {window} days",
        f"min(Temp9am) over last {window} days"
    ]
)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Fexpress elapsed time: {elapsed_time} seconds")
print(features.head())
print(features.shape)
