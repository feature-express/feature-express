<img src="https://feature.express/img/logo_blocky.png" width="100%"/>



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

    Requirement already satisfied: fexpress in /Users/pawel/.pyenv/versions/3.9.16/envs/fexpress/lib/python3.9/site-packages (0.0.1)


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
        "MinTemp": "Num",
        "WindSpeed9am": "Num",
        "WindDir3pm": "Str",
        "WindSpeed3pm": "Num",
        "Temp3pm": "Num",
        "WindDir9am": "Str",
        "RainToday": "Str",
        "WindGustSpeed": "Num",
        "MaxTemp": "Num",
        "Pressure9am": "Num",
        "Cloud9am": "Num",
        "WindGustDir": "Str",
        "Evaporation": "Num",
        "Cloud3pm": "Num",
        "Pressure3pm": "Num",
        "Humidity3pm": "Num",
        "Humidity9am": "Num",
        "Rainfall": "Num",
        "Sunshine": "Num",
        "Temp9am": "Num"
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

                        obs_dt          city  avg(MaxTemp) over last 7 days   
    0  2008-12-31 23:59:59.999  CoffsHarbour                            NaN  \
    1  2009-01-07 23:59:59.999  CoffsHarbour                      27.471430   
    2  2009-01-14 23:59:59.999  CoffsHarbour                      26.014286   
    3  2009-01-21 23:59:59.999  CoffsHarbour                      27.042858   
    4  2009-01-28 23:59:59.999  CoffsHarbour                      28.114285   
    
       min(MinTemp) over last 7 days  max(WindGustSpeed) over last 3 days   
    0                            NaN                                  NaN  \
    1                           14.8                                 56.0   
    2                           13.7                                 43.0   
    3                           14.3                                 59.0   
    4                           20.1                                 31.0   
    
       last(Humidity3pm) over past  first(Humidity9am) over future   
    0                          NaN                            51.0  \
    1                         66.0                            70.0   
    2                         49.0                            50.0   
    3                         61.0                            76.0   
    4                         66.0                            74.0   
    
       sum(Rainfall) over last 30 days  avg(WindSpeed9am) over last 5 days   
    0                         0.000000                                 NaN  \
    1                         5.400000                                15.6   
    2                        16.400002                                14.4   
    3                        30.000000                                15.4   
    4                        41.200001                                19.0   
    
       avg(WindSpeed3pm) over last 5 days  ...  first(Temp9am) over last 3 days   
    0                                 NaN  ...                              NaN  \
    1                           28.400000  ...                        24.600000   
    2                           19.799999  ...                        24.799999   
    3                           28.000000  ...                        22.299999   
    4                           19.400000  ...                        23.000000   
    
       rainy_days  non_rainy_days  avg(Cloud9am) over last 7 days   
    0           0               0                             NaN  \
    1           1               6                        3.857143   
    2           3              11                        4.285714   
    3           5              16                        5.000000   
    4           8              20                        5.428571   
    
       avg(Cloud3pm) over last 7 days  sum(Pressure9am) over last 3 days   
    0                             NaN                           0.000000  \
    1                        4.714286                        3040.900146   
    2                        3.571429                        3051.399902   
    3                        5.714286                        3051.700195   
    4                        4.714286                        3054.800049   
    
       sum(Pressure3pm) over last 3 days  last(WindGustDir) over past   
    0                           0.000000                         None  \
    1                        3033.399902                          NNE   
    2                        3047.399902                            N   
    3                        3042.800049                          NNE   
    4                        3052.100098                          SSW   
    
      max(Temp3pm) over last 7 days  min(Temp9am) over last 7 days  
    0                           NaN                            NaN  
    1                          28.4                      21.700001  
    2                          26.4                      20.000000  
    3                          27.6                      19.100000  
    4                          28.6                      22.299999  
    
    [5 rows x 21 columns]
    CPU times: user 32.3 s, sys: 335 ms, total: 32.6 s
    Wall time: 11 s

