from dataclasses import dataclass
from typing import List, Any, Union, TypeVar, Callable, Type, cast
from enum import Enum


T = TypeVar("T")
EnumT = TypeVar("EnumT", bound=Enum)


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


def from_str(x: Any) -> str:
    assert isinstance(x, str)
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


def to_enum(c: Type[EnumT], x: Any) -> EnumT:
    assert isinstance(x, c)
    return x.value


@dataclass
class EventQueryConfigClass:
    related_entities_events: List[str]

    @staticmethod
    def from_dict(obj: Any) -> 'EventQueryConfigClass':
        assert isinstance(obj, dict)
        related_entities_events = from_list(from_str, obj.get("RelatedEntitiesEvents"))
        return EventQueryConfigClass(related_entities_events)

    def to_dict(self) -> dict:
        result: dict = {}
        result["RelatedEntitiesEvents"] = from_list(from_str, self.related_entities_events)
        return result


class EventQueryConfigEnum(Enum):
    ALL_EVENTS = "AllEvents"


def event_query_config_from_dict(s: Any) -> Union[EventQueryConfigClass, EventQueryConfigEnum]:
    return from_union([EventQueryConfigClass.from_dict, EventQueryConfigEnum], s)


def event_query_config_to_dict(x: Union[EventQueryConfigClass, EventQueryConfigEnum]) -> Any:
    return from_union([lambda x: to_class(EventQueryConfigClass, x), lambda x: to_enum(EventQueryConfigEnum, x)], x)
