import json

import fexpress as fx
import pandas as pd


def test_event_creation():
    # Example dataframe for the test
    df = pd.DataFrame(
        {
            "Location": ["Sydney"],
            "Date": ["2023-08-01"],
            "MinTemp": [10],
            "MaxTemp": [20],
        }
    )

    event_context = fx.FeatureExpress()
    for row in df.itertuples():
        event = fx.Event(
            event_id=str(row.Index),
            entities={"city": row.Location},
            event_type="reading",
            event_time=str(row.Date),
            attrs={"MinTemp": row.MinTemp, "MaxTemp": row.MaxTemp},
        )
        event_context.new_event(event)

    # Assuming you have a method to retrieve events, adjust as needed
    events = event_context.events()
    events = [json.loads(event) for event in events]
    print(events)
    assert len(events) == 1
    assert events[0]["event_id"] == "0"
    assert events[0]["entities"]["city"] == "Sydney"
    assert events[0]["attrs"]["MinTemp"] == 10
    assert events[0]["attrs"]["MaxTemp"] == 20


def test_query_features():
    # Example dataframe for the test
    df = pd.DataFrame(
        {
            "Location": ["Sydney"],
            "Date": ["2023-08-01"],
            "MinTemp": [10],
            "MaxTemp": [20],
        }
    )

    event_context = fx.FeatureExpress()
    for row in df.itertuples():
        event = fx.Event(
            event_id=str(row.Index),
            entities={"city": row.Location},
            event_type="reading",
            event_time=str(row.Date),
            attrs={"MinTemp": row.MinTemp, "MaxTemp": row.MaxTemp},
        )
        event_context.new_event(event)

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
        include_events_on_obs_date=False, parallel=True
    )

    features = event_context.query(
        obs_dates_config=obs_dates_config,
        event_scope_config=event_scope_config,
        query_config=query_config,
        query=[
            "obs_dt as obs_dt",
            "@entities.city as city",
            "avg(MaxTemp) over last 7 days",
            "min(MinTemp) over last 7 days",
        ],
    )

    assert "city" in features.columns
    assert "obs_dt" in features.columns
    assert "avg(MaxTemp) over last 7 days" in features.columns
    assert "min(MinTemp) over last 7 days" in features.columns
