from dataclasses import dataclass
from typing import Any, TypeVar, Type, cast


T = TypeVar("T")


def from_bool(x: Any) -> bool:
    assert isinstance(x, bool)
    return x


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


@dataclass
class EventStoreSettings:
    include_events_on_obs_date: bool
    parallel: bool

    @staticmethod
    def from_dict(obj: Any) -> 'EventStoreSettings':
        assert isinstance(obj, dict)
        include_events_on_obs_date = from_bool(obj.get("include_events_on_obs_date"))
        parallel = from_bool(obj.get("parallel"))
        return EventStoreSettings(include_events_on_obs_date, parallel)

    def to_dict(self) -> dict:
        result: dict = {}
        result["include_events_on_obs_date"] = from_bool(self.include_events_on_obs_date)
        result["parallel"] = from_bool(self.parallel)
        return result


def event_store_settings_from_dict(s: Any) -> EventStoreSettings:
    return EventStoreSettings.from_dict(s)


def event_store_settings_to_dict(x: EventStoreSettings) -> Any:
    return to_class(EventStoreSettings, x)
