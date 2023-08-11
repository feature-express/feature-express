import json
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import List

import pandas as pd

from fexpress_python.fexpress_python import EventContext

from fexpress_python.sdk.event_store_settings import EventStoreSettings
from fexpress_python.sdk.event import Event
from fexpress_python.sdk.observation_dates_config import ObservationDatesConfigClass as ObservationDateConfig, \
    observation_dates_config_to_dict
from fexpress_python.sdk.event_query_config import event_query_config_to_dict


class FeatureExpress:

    def __init__(self, settings: EventStoreSettings):
        self.settings = settings
        self.event_context = EventContext(json.dumps(settings.to_dict()))

    def new_event(self, event: Event):
        event_json = json.dumps(event.to_dict())
        self.event_context.new_json_event(event_json)

    def query(self, obs_dates_config: ObservationDateConfig, event_query_config, query: str) -> pd.DataFrame:
        obs_dates_config_json = json.dumps(observation_dates_config_to_dict(obs_dates_config))
        event_query_config_json = json.dumps(event_query_config_to_dict(event_query_config))
        data = self.event_context.query(
            obs_dates_config_json=obs_dates_config_json,
            event_query_config_json=event_query_config_json,
            query=query
        )
        column_names, rows = data
        return pd.DataFrame(rows, columns=column_names)

    def flush(self):
        self.event_context.flush()

    def flush_experiments(self):
        self.event_context.flush_experiments()

    def flush_experiment(self, experiment_id):
        self.event_context.flush_experiment(experiment_id)


#
# from .fexpress import EventContext
#
#
# class FeatureType(Enum):
#     NUMERIC = "num_features"
#     CATEGORICAL = "cat_features"
#     BAG_OF_WORDS = "bow_features"
#     LIST = "list_features"
#     DICTIONARY = "dict_features"
#     OTHER = "other"
#
#
# class BaseEventEmitter:
#     def emit(self, event: Event) -> List[Event]:
#         pass
#
#
# @dataclass
# class Feature:
#     expr: str
#     alias: str = None
#     type: FeatureType = None
#
#     def get_expr(self):
#         if self.alias:
#             return "{} as {}".format(self.expr, self.alias)
#         else:
#             return self.expr
#
#     def get_name(self):
#         if self.alias:
#             return self.alias
#         else:
#             return self.expr
#
#
# class FExpress:
#
#     def __init__(self,
#                  event_emitters: List[BaseEventEmitter] = None,
#                  postgres_config=None,
#                  only_increasing_time_for_event_emitters=True,
#                  derived_events_keep_experiment_id=True,
#                  derived_events_keep_time=True):
#         """
#         :param event_emitters: List of event emitters
#         :param postgres_config: (optional) postgres config
#         :param only_increasing_time_for_event_emitters: if the event emitters are non empty it checks for increasing time
#         :param derived_events_keep_experiment_id: (optional) saves the experiment_id from the original event
#         :param derived_events_keep_time: (optional) saves the time from the original event
#         """
#         self.event_context = EventContext(postgres_config)
#         self.event_emitters = event_emitters or []
#         self.only_increasing_time_for_event_emitters = only_increasing_time_for_event_emitters
#         self.derived_events_keep_experiment_id = derived_events_keep_experiment_id
#         self.derived_events_keep_time = derived_events_keep_time
#         self.last_time = None
#         self.event_types = Counter()
#
#     # noinspection PyTypeChecker
#     def new_event(self, event):
#         self.validate_event(event)
#         self.event_types.update([event.get_event_type()])
#         self.event_context.new_event(event)
#         for event_emitter in self.event_emitters:
#             for new_event in event_emitter.emit(event):
#                 if self.derived_events_keep_experiment_id:
#                     new_event.set_experiment_id(event.get_experiment_id())
#                 if self.derived_events_keep_time:
#                     new_event.set_time(event.get_time())
#                 self.event_context.new_event(new_event)
#
#     def validate_event(self, event: Event):
#         if self.only_increasing_time_for_event_emitters and self.event_emitters:
#             if self.last_time is None:
#                 self.last_time = event.get_time()
#             else:
#                 event_time = event.get_time()
#                 if event_time < self.last_time:
#                     raise ValueError("Event time should be only increasing over using event emitters")
#                 self.last_time = event.get_time()
#
#     def extract_features(self, features_def: List[Feature], obs_dates, experiment_id=None, chunk_size=None):
#         features = self.event_context.extract_features(
#             obs_dates,
#             [f.get_expr() for f in features_def],
#             experiment_id,
#             chunk_size
#         )
#         df = pd.DataFrame.from_records(features)
#         df = df[[f.get_name() for f in features_def]]
#         return df
#
#     def extract_features_as_records(self, features_def: List[Feature], obs_dates, experiment_id=None):
#         """
#         Returns raw records and feature names from which you can construct a DataFrame
#         """
#         columns, records = self.event_context.extract_features_as_records(
#             obs_dates,
#             [f.get_expr() for f in features_def],
#             experiment_id
#         )
#         return records, columns
#
#     def flush(self):
#         self.event_context.flush()
#
#     def flush_experiments(self):
#         self.event_context.flush_experiments()
#
#     def flush_experiment(self, experiment_id):
#         self.event_context.flush_experiment(experiment_id)
