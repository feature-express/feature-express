import json
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import List

import pandas as pd

from fexpress.fexpress import EventContext

from fexpress.sdk.event import Event
from fexpress.sdk.query_config import QueryConfig
from fexpress.sdk.observation_dates_config import (
    ObservationDatesConfigClass as ObservationDateConfig,
    observation_dates_config_to_dict,
)
from fexpress.sdk.event_scope_config import event_scope_config_to_dict


class FeatureExpress:
    def __init__(self):
        self.event_context = EventContext()

    def new_event(self, event: Event):
        event_json = json.dumps(event.to_dict())
        self.event_context.new_json_event(event_json)

    def query(
        self,
        obs_dates_config: ObservationDateConfig,
        event_scope_config,
        query_config,
        query,
    ) -> pd.DataFrame:
        obs_dates_config_json = json.dumps(
            observation_dates_config_to_dict(obs_dates_config)
        )
        event_scope_config_json = json.dumps(
            event_scope_config_to_dict(event_scope_config)
        )
        query_config_json = json.dumps(query_config.to_dict())
        data = self.event_context.query(
            obs_dates_config_json=obs_dates_config_json,
            event_scope_config_json=event_scope_config_json,
            query_config_json=query_config_json,
            query=query,
        )
        column_names, rows = data
        return pd.DataFrame(rows, columns=column_names)

    def __getattr__(self, name):
        # Check if the attribute is a callable method on the event_context
        attr = getattr(self.event_context, name)
        if callable(attr):
            # If it is, define a wrapper function that forwards the call
            def wrapper(*args, **kwargs):
                return attr(*args, **kwargs)

            return wrapper
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )
