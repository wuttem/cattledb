#!/usr/bin/python
# coding: utf8

import logging
import time
import os
import random

from .engines import engine_factory, get_engine_capabilities
from ..core.models import MetricDefinition, EventDefinition
from ..core.helper import merge_lists_on_key

logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, project_id, instance_id, read_only=False, pool_size=8, table_prefix="mycdb",
                 credentials=None, metric_definition=None, event_definitions=None, engine="bigtable"):
        self.project_id = project_id
        self.instance_id = instance_id
        self.read_only = read_only
        self.table_prefix = table_prefix
        self.credentials = credentials
        self.engine_type = engine
        self.data_dir = "."

        self.engine_capabilities = get_engine_capabilities(self.engine_type)

        self.engines = []
        self.admin_engine = None

        if self.engine_capabilities.get("threading"):
            self.pool_size = pool_size
        else:
            self.pool_size = 1

        self.stores = {}

        self.metrics = []
        if metric_definition is not None:
            self.metrics += metric_definition

        self.event_definitions = []
        if event_definitions is not None:
            self.event_definitions += event_definitions

        # Register Default Data Stores
        from .stores import ConfigStore
        self._config_store = ConfigStore(self)
        self.register_store(self._config_store)
        from .stores import TimeSeriesStore
        self.timeseries = TimeSeriesStore(self)
        self.register_store(self.timeseries)
        from .stores import ActivityStore
        self.activity = ActivityStore(self)
        self.register_store(self.activity)
        from .stores import EventStore
        self.events = EventStore(self)
        self.register_store(self.events)
        from .stores import MetaDataStore
        self.metadata = MetaDataStore(self)
        self.register_store(self.metadata)

    def clone(self):
        return Connection(self.project_id, self.instance_id, read_only=self.read_only,
                          pool_size=self.pool_size, table_prefix=self.table_prefix,
                          credentials=self.credentials, metric_definition=self.metrics,
                          event_definitions=self.event_definitions)

    def register_store(self, store):
        self.stores[store.STOREID] = store

    def create_tables(self, silent=False):
        eng = self.get_admin_engine()
        for s in self.stores.values():
            table_def = s.get_table_definitions()
            for table_name, columns in table_def.items():
                eng.setup_table(table_name, silent=silent)
                for col in columns:
                    eng.setup_column_family(table_name, column_family=col, silent=silent)

    def create_all_metrics(self, silent=False):
        eng = self.get_admin_engine()
        table_name = self.timeseries.TABLENAME
        for m in self.metrics:
            eng.setup_column_family(table_name, column_family=m.id, silent=silent)

    def create_metric(self, metric_name, silent=False):
        eng = self.get_admin_engine()
        table_name = self.timeseries.TABLENAME
        for m in self.metrics:
            if m.name == metric_name:
                eng.setup_column_family(table_name, column_family=m.id, silent=silent)
                break
            elif m.id == metric_name:
                eng.setup_column_family(table_name, column_family=m.id, silent=silent)
                break
        else:
            raise KeyError("metric {} not known (add it to settings)".format(metric_name))

    def get_engine(self):
        if len(self.engines) < 1:
            eng = engine_factory(self.engine_type, read_only=self.read_only, table_prefix=self.table_prefix,
                                 admin=False, engine_options={"project_id": self.project_id,
                                                              "instance_id": self.instance_id,
                                                              "credentials": self.credentials,
                                                              "data_dir": self.data_dir})
            logger.info("New Database Engine Connection created")
            self.engines.append(eng)
            return eng
        return random.choice(self.engines)

    def get_admin_engine(self):
        if self.admin_engine is None:
            self.admin_engine = engine_factory(self.engine_type, read_only=self.read_only, table_prefix=self.table_prefix,
                                               admin=True, engine_options={"project_id": self.project_id,
                                                                           "instance_id": self.instance_id,
                                                                           "credentials": self.credentials,
                                                                           "data_dir": self.data_dir})
            logger.warning("New Admin Database Engine Connection created")
        return self.admin_engine

    # Table Access Methods
    def get_table(self, table_name):
        eng = self.get_engine()
        return eng.get_table(table_name)

    # Shared Methods
    def write_cell(self, table_id, row_id, column, value):
        t = self.get_table(table_id)
        return t.write_cell(row_id, column, value)

    def read_row(self, table_id, row_id):
        t = self.get_table(table_id)
        return t.read_row(row_id)

    # config
    def write_config(self, key, value):
        return self._config_store.put(key, value)

    def read_config(self, key):
        return self._config_store.get(key)

    def store_metric_definitions(self):
        data = []
        for m in self.metrics:
            data.append(m.to_dict())
        self.write_config("metrics", data)

    def _get_metric_definitions(self):
        try:
            data = self.read_config("metrics")
        except KeyError:
            return []
        metrics = [MetricDefinition.from_dict(m) for m in data]
        return metrics

    def load_metric_definitions(self):
        m_new = self._get_metric_definitions()
        merged = merge_lists_on_key(self.metrics, m_new, key=lambda x: x.name)
        self.metrics = merged

    def store_event_definitions(self):
        data = []
        for e in self.event_definitions:
            data.append(e.to_dict())
        self.write_config("events", data)

    def _get_event_definitions(self):
        try:
            data = self.read_config("events")
        except KeyError:
            return []
        events = [EventDefinition.from_dict(e) for e in data]
        return events

    def load_event_definitions(self):
        e_new = self._get_event_definitions()
        merged = merge_lists_on_key(self.event_definitions, e_new, key=lambda x: x.name)
        self.event_definitions = merged
