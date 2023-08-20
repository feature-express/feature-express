from fexpress import Event, EventStoreSettings, ObservationDateConfig
from fexpress import FeatureExpress
from fexpress.sdk.observation_dates_config import (
    Interval,
    DatePart,
    ObservationDatesConfigEnum,
)

if __name__ == "__main__":
    settings = EventStoreSettings(include_events_on_obs_date=True, parallel=False)
    fx = FeatureExpress(settings)

    event = Event(
        event_id="1",
        entities={"home": "a", "away": "b"},
        event_type="pressure",
        event_time="2020-01-01T16:39:57",
        attrs={"pressure": 10.0},
    )
    fx.new_event(event)

    event = Event(
        event_id="1",
        entities={"home": "c", "away": "d"},
        event_type="pressure",
        event_time="2020-01-02T16:39:57",
        attrs={"pressure": 10.0},
    )
    fx.new_event(event)

    # obs_dates_config = ObservationDateConfig(
    #     interval=Interval(date_part=DatePart.WEEK, entity_types=["user"], nth=7),
    # )

    obs_dates_config = ObservationDatesConfigEnum.ALL_EVENTS

    df = fx.query(
        obs_dates_config=obs_dates_config,
        query="""
        SELECT
            obs_dt as obs_dt,
            entities.home as home,
            entities.away as away,
            AVG(pressure) over past as pressure
        FOR
            @entities := user
        """,
    )

    print(df)
