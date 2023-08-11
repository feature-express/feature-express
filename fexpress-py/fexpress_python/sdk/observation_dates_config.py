from dataclasses import dataclass
from typing import List, Any, Dict, Optional, Union, TypeVar, Callable, Type, cast
from enum import Enum


T = TypeVar("T")
EnumT = TypeVar("EnumT", bound=Enum)


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


def from_dict(f: Callable[[Any], T], x: Any) -> Dict[str, T]:
    assert isinstance(x, dict)
    return { k: f(v) for (k, v) in x.items() }


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_union(fs, x):
    for f in fs:
        try:
            return f(x)
        except:
            pass
    assert False


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


def from_int(x: Any) -> int:
    assert isinstance(x, int) and not isinstance(x, bool)
    return x


def to_enum(c: Type[EnumT], x: Any) -> EnumT:
    assert isinstance(x, c)
    return x.value


@dataclass
class ConditionalEvents:
    """This type of observation dates means that the observation dates are based on a specific
    condition based on the where clause applied to the [Event]. Probably the proper way to
    handle this type of thing. Is that the user provides the where clause and we only compile
    it when we need to calculate the features.
    """
    condition: str
    entity_types: List[str]

    @staticmethod
    def from_dict(obj: Any) -> 'ConditionalEvents':
        assert isinstance(obj, dict)
        condition = from_str(obj.get("condition"))
        entity_types = from_list(from_str, obj.get("entity_types"))
        return ConditionalEvents(condition, entity_types)

    def to_dict(self) -> dict:
        result: dict = {}
        result["condition"] = from_str(self.condition)
        result["entity_types"] = from_list(from_str, self.entity_types)
        return result


@dataclass
class EntitiesEventSpecific:
    """This struct represents the choice of specific events that will serve as the source for
    the observation dates and also be a source of the event.
    """
    dates: Dict[str, List[List[str]]]

    @staticmethod
    def from_dict(obj: Any) -> 'EntitiesEventSpecific':
        assert isinstance(obj, dict)
        dates = from_dict(lambda x: from_list(lambda x: from_list(from_str, x), x), obj.get("dates"))
        return EntitiesEventSpecific(dates)

    def to_dict(self) -> dict:
        result: dict = {}
        result["dates"] = from_dict(lambda x: from_list(lambda x: from_list(from_str, x), x), self.dates)
        return result


@dataclass
class ObservationTime:
    observation_time_datetime: str
    event_id: Optional[str] = None

    @staticmethod
    def from_dict(obj: Any) -> 'ObservationTime':
        assert isinstance(obj, dict)
        observation_time_datetime = from_str(obj.get("datetime"))
        event_id = from_union([from_none, from_str], obj.get("event_id"))
        return ObservationTime(observation_time_datetime, event_id)

    def to_dict(self) -> dict:
        result: dict = {}
        result["datetime"] = from_str(self.observation_time_datetime)
        if self.event_id is not None:
            result["event_id"] = from_union([from_none, from_str], self.event_id)
        return result


@dataclass
class EntitySpecific:
    dates: Dict[str, List[ObservationTime]]

    @staticmethod
    def from_dict(obj: Any) -> 'EntitySpecific':
        assert isinstance(obj, dict)
        dates = from_dict(lambda x: from_list(ObservationTime.from_dict, x), obj.get("dates"))
        return EntitySpecific(dates)

    def to_dict(self) -> dict:
        result: dict = {}
        result["dates"] = from_dict(lambda x: from_list(lambda x: to_class(ObservationTime, x), x), self.dates)
        return result


@dataclass
class Fixed:
    dates: List[str]
    entity_types: List[str]

    @staticmethod
    def from_dict(obj: Any) -> 'Fixed':
        assert isinstance(obj, dict)
        dates = from_list(from_str, obj.get("dates"))
        entity_types = from_list(from_str, obj.get("entity_types"))
        return Fixed(dates, entity_types)

    def to_dict(self) -> dict:
        result: dict = {}
        result["dates"] = from_list(from_str, self.dates)
        result["entity_types"] = from_list(from_str, self.entity_types)
        return result


class DatePart(Enum):
    ALL = "All"
    DAY = "Day"
    HOUR = "Hour"
    MILLISECOND = "Millisecond"
    MINUTE = "Minute"
    SECOND = "Second"
    WEEK = "Week"


@dataclass
class Interval:
    date_part: DatePart
    entity_types: List[str]
    nth: int

    @staticmethod
    def from_dict(obj: Any) -> 'Interval':
        assert isinstance(obj, dict)
        date_part = DatePart(obj.get("date_part"))
        entity_types = from_list(from_str, obj.get("entity_types"))
        nth = from_int(obj.get("nth"))
        return Interval(date_part, entity_types, nth)

    def to_dict(self) -> dict:
        result: dict = {}
        result["date_part"] = to_enum(DatePart, self.date_part)
        result["entity_types"] = from_list(from_str, self.entity_types)
        result["nth"] = from_int(self.nth)
        return result


@dataclass
class ObservationDatesConfigClass:
    interval: Optional[Interval] = None
    fixed: Optional[Fixed] = None
    entity_specific: Optional[EntitySpecific] = None
    all_events_by_entity: Optional[List[str]] = None
    conditional_events: Optional[ConditionalEvents] = None
    entities_event_specific: Optional[EntitiesEventSpecific] = None

    @staticmethod
    def from_dict(obj: Any) -> 'ObservationDatesConfigClass':
        assert isinstance(obj, dict)
        interval = from_union([Interval.from_dict, from_none], obj.get("Interval"))
        fixed = from_union([Fixed.from_dict, from_none], obj.get("Fixed"))
        entity_specific = from_union([EntitySpecific.from_dict, from_none], obj.get("EntitySpecific"))
        all_events_by_entity = from_union([lambda x: from_list(from_str, x), from_none], obj.get("AllEventsByEntity"))
        conditional_events = from_union([ConditionalEvents.from_dict, from_none], obj.get("ConditionalEvents"))
        entities_event_specific = from_union([EntitiesEventSpecific.from_dict, from_none], obj.get("EntitiesEventSpecific"))
        return ObservationDatesConfigClass(interval, fixed, entity_specific, all_events_by_entity, conditional_events, entities_event_specific)

    def to_dict(self) -> dict:
        result: dict = {}
        if self.interval is not None:
            result["Interval"] = from_union([lambda x: to_class(Interval, x), from_none], self.interval)
        if self.fixed is not None:
            result["Fixed"] = from_union([lambda x: to_class(Fixed, x), from_none], self.fixed)
        if self.entity_specific is not None:
            result["EntitySpecific"] = from_union([lambda x: to_class(EntitySpecific, x), from_none], self.entity_specific)
        if self.all_events_by_entity is not None:
            result["AllEventsByEntity"] = from_union([lambda x: from_list(from_str, x), from_none], self.all_events_by_entity)
        if self.conditional_events is not None:
            result["ConditionalEvents"] = from_union([lambda x: to_class(ConditionalEvents, x), from_none], self.conditional_events)
        if self.entities_event_specific is not None:
            result["EntitiesEventSpecific"] = from_union([lambda x: to_class(EntitiesEventSpecific, x), from_none], self.entities_event_specific)
        return result


class ObservationDatesConfigEnum(Enum):
    ALL_EVENTS = "AllEvents"


def observation_dates_config_from_dict(s: Any) -> Union[ObservationDatesConfigClass, ObservationDatesConfigEnum]:
    return from_union([ObservationDatesConfigClass.from_dict, ObservationDatesConfigEnum], s)


def observation_dates_config_to_dict(x: Union[ObservationDatesConfigClass, ObservationDatesConfigEnum]) -> Any:
    return from_union([lambda x: to_class(ObservationDatesConfigClass, x), lambda x: to_enum(ObservationDatesConfigEnum, x)], x)
