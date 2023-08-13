from dataclasses import dataclass
from typing import Optional, Any, TypeVar, Type, cast


T = TypeVar("T")


def from_bool(x: Any) -> bool:
    assert isinstance(x, bool)
    return x


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


@dataclass
class QueryConfig:
    include_events_on_obs_date: Optional[bool] = None
    parallel: Optional[bool] = None

    @staticmethod
    def from_dict(obj: Any) -> 'QueryConfig':
        assert isinstance(obj, dict)
        include_events_on_obs_date = from_union([from_bool, from_none], obj.get("include_events_on_obs_date"))
        parallel = from_union([from_bool, from_none], obj.get("parallel"))
        return QueryConfig(include_events_on_obs_date, parallel)

    def to_dict(self) -> dict:
        result: dict = {}
        if self.include_events_on_obs_date is not None:
            result["include_events_on_obs_date"] = from_union([from_bool, from_none], self.include_events_on_obs_date)
        if self.parallel is not None:
            result["parallel"] = from_union([from_bool, from_none], self.parallel)
        return result


def query_config_from_dict(s: Any) -> QueryConfig:
    return QueryConfig.from_dict(s)


def query_config_to_dict(x: QueryConfig) -> Any:
    return to_class(QueryConfig, x)
