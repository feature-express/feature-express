from dataclasses import dataclass
from typing import List, Union, Optional, Any, Dict, TypeVar, Callable, Type, cast


T = TypeVar("T")


def from_float(x: Any) -> float:
    assert isinstance(x, (float, int)) and not isinstance(x, bool)
    return float(x)


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_bool(x: Any) -> bool:
    assert isinstance(x, bool)
    return x


def from_int(x: Any) -> int:
    assert isinstance(x, int) and not isinstance(x, bool)
    return x


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


def to_float(x: Any) -> float:
    assert isinstance(x, float)
    return x


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


def from_dict(f: Callable[[Any], T], x: Any) -> Dict[str, T]:
    assert isinstance(x, dict)
    return {k: f(v) for (k, v) in x.items()}


@dataclass
class ValueWithAlias:
    alias: Optional[
        Union[
            float, bool, int, "ValueWithAlias", List[Union[bool, float, int, str]], str
        ]
    ] = None
    value: Optional[
        Union[
            float, bool, int, "ValueWithAlias", List[Union[bool, float, int, str]], str
        ]
    ] = None

    @staticmethod
    def from_dict(obj: Any) -> "ValueWithAlias":
        assert isinstance(obj, dict)
        alias = from_union(
            [
                from_float,
                from_none,
                from_bool,
                from_int,
                ValueWithAlias.from_dict,
                lambda x: from_list(
                    lambda x: from_union(
                        [from_bool, from_float, from_int, from_str], x
                    ),
                    x,
                ),
                from_str,
            ],
            obj.get("alias"),
        )
        value = from_union(
            [
                from_float,
                from_none,
                from_bool,
                from_int,
                ValueWithAlias.from_dict,
                lambda x: from_list(
                    lambda x: from_union(
                        [from_bool, from_float, from_int, from_str], x
                    ),
                    x,
                ),
                from_str,
            ],
            obj.get("value"),
        )
        return ValueWithAlias(alias, value)

    def to_dict(self) -> dict:
        result: dict = {}
        if self.alias is not None:
            result["alias"] = from_union(
                [
                    to_float,
                    from_none,
                    from_bool,
                    from_int,
                    lambda x: to_class(ValueWithAlias, x),
                    lambda x: from_list(
                        lambda x: from_union(
                            [from_bool, to_float, from_int, from_str], x
                        ),
                        x,
                    ),
                    from_str,
                ],
                self.alias,
            )
        if self.value is not None:
            result["value"] = from_union(
                [
                    to_float,
                    from_none,
                    from_bool,
                    from_int,
                    lambda x: to_class(ValueWithAlias, x),
                    lambda x: from_list(
                        lambda x: from_union(
                            [from_bool, to_float, from_int, from_str], x
                        ),
                        x,
                    ),
                    from_str,
                ],
                self.value,
            )
        return result


@dataclass
class Event:
    """This is the original format of the event"""

    entities: Dict[str, str]
    event_time: str
    event_type: str
    attrs: Optional[
        Dict[
            str,
            Optional[
                Union[
                    float,
                    bool,
                    int,
                    ValueWithAlias,
                    List[Union[bool, float, int, str]],
                    str,
                ]
            ],
        ]
    ] = None
    event_id: Optional[str] = None
    experiment_id: Optional[str] = None

    @staticmethod
    def from_dict(obj: Any) -> "Event":
        assert isinstance(obj, dict)
        entities = from_dict(from_str, obj.get("entities"))
        event_time = from_str(obj.get("event_time"))
        event_type = from_str(obj.get("event_type"))
        attrs = from_union(
            [
                from_none,
                lambda x: from_dict(
                    lambda x: from_union(
                        [
                            from_float,
                            from_none,
                            from_bool,
                            from_int,
                            ValueWithAlias.from_dict,
                            lambda x: from_list(
                                lambda x: from_union(
                                    [from_bool, from_float, from_int, from_str], x
                                ),
                                x,
                            ),
                            from_str,
                        ],
                        x,
                    ),
                    x,
                ),
            ],
            obj.get("attrs"),
        )
        event_id = from_union([from_none, from_str], obj.get("event_id"))
        experiment_id = from_union([from_none, from_str], obj.get("experiment_id"))
        return Event(entities, event_time, event_type, attrs, event_id, experiment_id)

    def to_dict(self) -> dict:
        result: dict = {}
        result["entities"] = from_dict(from_str, self.entities)
        result["event_time"] = from_str(self.event_time)
        result["event_type"] = from_str(self.event_type)
        if self.attrs is not None:
            result["attrs"] = from_union(
                [
                    from_none,
                    lambda x: from_dict(
                        lambda x: from_union(
                            [
                                to_float,
                                from_none,
                                from_bool,
                                from_int,
                                lambda x: to_class(ValueWithAlias, x),
                                lambda x: from_list(
                                    lambda x: from_union(
                                        [from_bool, to_float, from_int, from_str], x
                                    ),
                                    x,
                                ),
                                from_str,
                            ],
                            x,
                        ),
                        x,
                    ),
                ],
                self.attrs,
            )
        if self.event_id is not None:
            result["event_id"] = from_union([from_none, from_str], self.event_id)
        if self.experiment_id is not None:
            result["experiment_id"] = from_union(
                [from_none, from_str], self.experiment_id
            )
        return result


def event_from_dict(s: Any) -> Event:
    return Event.from_dict(s)


def event_to_dict(x: Event) -> Any:
    return to_class(Event, x)
