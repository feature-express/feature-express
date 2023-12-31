# Modeling timeseries data

This is a basic example how to convert the data into events and create a declaration of features

## Importing Libraries

Here, we are importing pandas for data manipulation and `fexpress` which refers to our custom Rust-based feature engineering library.


```python
import os
os.system("pip install fexpress")
import pandas as pd
import fexpress as fx
```

    Requirement already satisfied: fexpress in /Users/pawel/.pyenv/versions/3.9.16/envs/fexpress/lib/python3.9/site-packages (0.0.3)


    WARNING: You are using pip version 22.0.4; however, version 23.2.1 is available.
    You should consider upgrading via the '/Users/pawel/.pyenv/versions/3.9.16/envs/fexpress/bin/python3.9 -m pip install --upgrade pip' command.


## Loading Data
We are loading rows from the "weatherAUS.csv" dataset and viewing its columns.


```python
if os.path.exists("/kaggle/input/weather-dataset-rattle-package/weatherAUS.csv"):
    df = pd.read_csv("/kaggle/input/weather-dataset-rattle-package/weatherAUS.csv", nrows=10000)
else:
    df = pd.read_csv("datasets/weatherAUS.csv", nrows=10000)
df.head()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr >
      <th></th>
      <th>Date</th>
      <th>Location</th>
      <th>MinTemp</th>
      <th>MaxTemp</th>
      <th>Rainfall</th>
      <th>Evaporation</th>
      <th>Sunshine</th>
      <th>WindGustDir</th>
      <th>WindGustSpeed</th>
      <th>WindDir9am</th>
      <th>...</th>
      <th>Humidity9am</th>
      <th>Humidity3pm</th>
      <th>Pressure9am</th>
      <th>Pressure3pm</th>
      <th>Cloud9am</th>
      <th>Cloud3pm</th>
      <th>Temp9am</th>
      <th>Temp3pm</th>
      <th>RainToday</th>
      <th>RainTomorrow</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2008-12-01</td>
      <td>Albury</td>
      <td>13.4</td>
      <td>22.9</td>
      <td>0.6</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>W</td>
      <td>44.0</td>
      <td>W</td>
      <td>...</td>
      <td>71.0</td>
      <td>22.0</td>
      <td>1007.7</td>
      <td>1007.1</td>
      <td>8.0</td>
      <td>NaN</td>
      <td>16.9</td>
      <td>21.8</td>
      <td>No</td>
      <td>No</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2008-12-02</td>
      <td>Albury</td>
      <td>7.4</td>
      <td>25.1</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>WNW</td>
      <td>44.0</td>
      <td>NNW</td>
      <td>...</td>
      <td>44.0</td>
      <td>25.0</td>
      <td>1010.6</td>
      <td>1007.8</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>17.2</td>
      <td>24.3</td>
      <td>No</td>
      <td>No</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2008-12-03</td>
      <td>Albury</td>
      <td>12.9</td>
      <td>25.7</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>WSW</td>
      <td>46.0</td>
      <td>W</td>
      <td>...</td>
      <td>38.0</td>
      <td>30.0</td>
      <td>1007.6</td>
      <td>1008.7</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>21.0</td>
      <td>23.2</td>
      <td>No</td>
      <td>No</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2008-12-04</td>
      <td>Albury</td>
      <td>9.2</td>
      <td>28.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NE</td>
      <td>24.0</td>
      <td>SE</td>
      <td>...</td>
      <td>45.0</td>
      <td>16.0</td>
      <td>1017.6</td>
      <td>1012.8</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>18.1</td>
      <td>26.5</td>
      <td>No</td>
      <td>No</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2008-12-05</td>
      <td>Albury</td>
      <td>17.5</td>
      <td>32.3</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>W</td>
      <td>41.0</td>
      <td>ENE</td>
      <td>...</td>
      <td>82.0</td>
      <td>33.0</td>
      <td>1010.8</td>
      <td>1006.0</td>
      <td>7.0</td>
      <td>8.0</td>
      <td>17.8</td>
      <td>29.7</td>
      <td>No</td>
      <td>No</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 23 columns</p>
</div>



## Creating Events
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

## Configuration
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

## Schema Information
Print the schema information for the reading event.


```python
print(event_context.event_context.schema())
```

    {
      "reading": {
        "Temp3pm": "Num",
        "Cloud3pm": "Num",
        "Evaporation": "Num",
        "Temp9am": "Num",
        "Sunshine": "Num",
        "WindDir9am": "Str",
        "MaxTemp": "Num",
        "Pressure3pm": "Num",
        "Cloud9am": "Num",
        "Rainfall": "Num",
        "WindDir3pm": "Str",
        "Pressure9am": "Num",
        "RainToday": "Str",
        "WindSpeed9am": "Num",
        "WindGustSpeed": "Num",
        "WindGustDir": "Str",
        "Humidity3pm": "Num",
        "WindSpeed3pm": "Num",
        "Humidity9am": "Num",
        "MinTemp": "Num"
      }
    }


## Querying Features
Using our defined configuration, we query features like average temperature, wind speed, rainfall, etc. over different time windows.


```python
%%time
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
```

                        obs_dt   city  avg(MaxTemp) over last 7 days   
    0  2008-12-31 23:59:59.999  Cobar                            NaN  \
    1  2009-01-07 23:59:59.999  Cobar                      35.899998   
    2  2009-01-14 23:59:59.999  Cobar                      36.757145   
    3  2009-01-21 23:59:59.999  Cobar                      35.871429   
    4  2009-01-28 23:59:59.999  Cobar                      36.542858   
    
       min(MinTemp) over last 7 days  max(WindGustSpeed) over last 3 days   
    0                            NaN                                  NaN  \
    1                      15.500000                                 43.0   
    2                      16.100000                                 43.0   
    3                      17.900000                                 59.0   
    4                      19.700001                                 46.0   
    
       last(Humidity3pm) over past  first(Humidity9am) over future   
    0                          NaN                            20.0  \
    1                         19.0                            33.0   
    2                         15.0                            24.0   
    3                         52.0                            71.0   
    4                         17.0                            31.0   
    
       sum(Rainfall) over last 30 days  avg(WindSpeed9am) over last 5 days   
    0                              NaN                                 NaN  \
    1                              0.0                                15.0   
    2                              0.0                                19.4   
    3                              4.8                                18.6   
    4                             31.4                                16.0   
    
       avg(WindSpeed3pm) over last 5 days  ...  first(Temp9am) over last 3 days   
    0                                 NaN  ...                              NaN  \
    1                           12.200000  ...                        29.100000   
    2                           10.600000  ...                        29.799999   
    3                           17.200001  ...                        26.200001   
    4                           10.800000  ...                        28.700001   
    
       rainy_days  non_rainy_days  avg(Cloud9am) over last 7 days   
    0           0               0                             NaN  \
    1           0               7                        2.333333   
    2           0              14                        1.000000   
    3           1              20                        2.428571   
    4           3              25                        2.571429   
    
       avg(Cloud3pm) over last 7 days  sum(Pressure9am) over last 3 days   
    0                             NaN                                NaN  \
    1                        4.571429                        3031.100098   
    2                        2.571429                        3039.100098   
    3                        4.142857                        3036.199951   
    4                        3.428571                        3044.200195   
    
       sum(Pressure3pm) over last 3 days  last(WindGustDir) over past   
    0                                NaN                         None  \
    1                        3023.899902                            N   
    2                        3030.600098                          SSW   
    3                        3027.899902                            N   
    4                        3036.199951                            E   
    
      max(Temp3pm) over last 7 days  min(Temp9am) over last 7 days  
    0                           NaN                            NaN  
    1                     37.599998                      20.299999  
    2                     38.099998                      20.700001  
    3                     37.799999                      19.900000  
    4                     38.700001                      24.400000  
    
    [5 rows x 21 columns]
    CPU times: user 411 ms, sys: 35.7 ms, total: 447 ms
    Wall time: 157 ms

