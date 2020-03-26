#!/usr/bin/python
# coding: utf-8

import logging
import time
import os
import threading
import warnings

from grpc import RpcError

from .engines import engine_factory, get_engine_capabilities
from ..core.models import MetricDefinition, EventDefinition
from ..core.helper import merge_lists_on_key

logger = logging.getLogger(__name__)





class Connection(object):
    MAX_THREADS = 1000

    def __init__(self, read_only=False, table_prefix="mycdb", engine_options=None,
                 metric_definitions=None, event_definitions=None, engine="bigtable",
                 admin=True, _config=None):
        self.read_only = read_only
        self.table_prefix = table_prefix
        self.engine_options = engine_options or {}
        self.engine_type = engine
        self.data_dir = "."
        self.admin = admin
        self.init = False
        self.config = _config

        self.engine_capabilities = get_engine_capabilities(self.engine_type)

        self.engines = {}
        self.thread_local = threading.local()
        # self.admin_engine = None

        self.threaded_engines = False
        if self.engine_capabilities.get("threading"):
            self.threaded_engines = True

        self.stores = {}

        self._metric_definitions = []
        if metric_definitions is not None:
            self.add_metric_definitions(metric_definitions)

        self._event_definitions = []
        if event_definitions is not None:
            self.add_event_definitions(event_definitions)

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

    @classmethod
    def from_config(cls, config):
        return cls(engine=config.ENGINE, engine_options=config.ENGINE_OPTIONS, table_prefix=config.TABLE_PREFIX,
                   read_only=config.READ_ONLY, admin=config.ADMIN, _config=config)

    def info(self):
        return {
            "name": "cattledb",
            "read_only": self.read_only,
            "admin": self.admin,
            "engine": self.engine_type,
            "stores": list(self.stores.keys()),
            "engine_pool": list(self.engines.keys()),
            "engine_pool_size": len(self.engines)
        }

    def register_store(self, store):
        self.stores[store.STOREID] = store

    def create_tables(self, silent=False):
        eng = self.get_engine()
        for s in self.stores.values():
            table_def = s.get_table_definitions()
            for table_name, columns in table_def.items():
                eng.setup_table(table_name, silent=silent)
                for col in columns:
                    eng.setup_column_family(table_name, column_family=col, silent=silent)

    def create_all_metrics(self, silent=False):
        eng = self.get_engine()
        table_name = self.timeseries.TABLENAME
        for m in self.metric_definitions:
            eng.setup_column_family(table_name, column_family=m.id, silent=silent)

    def create_metric(self, metric_name, silent=False):
        eng = self.get_engine()
        table_name = self.timeseries.TABLENAME
        for m in self.metric_definitions:
            if m.name == metric_name:
                eng.setup_column_family(table_name, column_family=m.id, silent=silent)
                break
            elif m.id == metric_name:
                eng.setup_column_family(table_name, column_family=m.id, silent=silent)
                break
        else:
            raise KeyError("metric {} not known (add it to settings)".format(metric_name))

    def _new_engine(self):
        return engine_factory(self.engine_type, read_only=self.read_only, table_prefix=self.table_prefix,
                              admin=self.admin, engine_options=self.engine_options)

    def get_engine(self):
        # only one engine if no threading
        if not self.threaded_engines:
            if "main" not in self.engines:
                self.engines["main"] = self._new_engine()
                logger.warning("New Database Engine created (Thread: {})".format("main"))
            return self.engines["main"]
        # check if this thread already has an engine
        try:
            engine = self.thread_local.engine
        except AttributeError:
            # no engine found for this thread
            t = threading.currentThread().getName()
            engine = self._new_engine()
            self.thread_local.engine = engine
            self.engines[t] = engine
            logger.warning("New Database Engine created (Thread: {})".format(t))
            if len(self.engines) > self.MAX_THREADS:
                logger.warning("MAX_THREAD sized reached with {} threads".format(len(self.engines)))
                raise RuntimeError("too many threads")
        return engine

    # def get_admin_engine(self):
    #     if self.admin_engine is None:
    #         self.admin_engine = self._new_engine(admin=True)
    #         logger.warning("New Admin Database Engine Connection created")
    #     return self.admin_engine

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

    def read_database_structure(self):
        all_tables = []
        eng = self.get_engine()
        for s in self.stores.values():
            table_def = s.get_table_definitions()
            for table_name, columns in table_def.items():
                entry = {
                    "name": table_name,
                    "full_name": eng.get_full_table_name(table_name),
                    "column_families": eng.get_admin_table(table_name).get_column_families()
                }
                all_tables.append(entry)
        return all_tables

    # service init
    # this is done as a warmup for services
    def service_init(self):
        self.restore_configuration()
        self.init = True

    def database_init(self, silent=False):
        if not silent:
            try:
                database_init = self.read_config("database_init")
            except (KeyError, RpcError) as e:
                pass
            else:
                raise RuntimeError("database is already initialized")
        self.create_tables(silent=silent)
        self.load_event_definitions()
        self.load_metric_definitions()
        self.store_event_definitions()
        self.store_metric_definitions()
        self.write_config("database_init", {"ts": int(time.time())})
        self.init = True
        self.create_all_metrics(silent=silent)

    def check_init(self, msg=None):
        if not self.init:
            if msg is not None:
                raise RuntimeError(msg)
            raise RuntimeError("connection is not initialized")

    # metric and event definitions
    @property
    def metric_definitions(self):
        self.check_init()
        return self._metric_definitions

    @property
    def event_definitions(self):
        self.check_init()
        return self._event_definitions

    def add_metric_definitions(self, defs):
        for d in defs:
            assert isinstance(d, MetricDefinition)
        self._metric_definitions = merge_lists_on_key(self._metric_definitions, defs, key=lambda x: x.id)

    def add_event_definitions(self, defs):
        for d in defs:
            assert isinstance(d, EventDefinition)
        self._event_definitions = merge_lists_on_key(self._event_definitions, defs, key=lambda x: x.name)

    def new_metric_definition(self, metric_def):
        self.check_init()
        assert isinstance(metric_def, MetricDefinition)
        name = metric_def.name
        self.load_metric_definitions()
        self.add_metric_definitions([metric_def])
        self.store_metric_definitions()
        self.create_metric(metric_def.name)

    def new_event_definition(self, event_def):
        self.check_init()
        assert isinstance(event_def, EventDefinition)
        self.load_event_definitions()
        self.add_event_definitions([event_def])
        self.store_event_definitions()

    # config
    def write_config(self, key, value):
        return self._config_store.put(key, value)

    def read_config(self, key):
        return self._config_store.get(key)

    def store_metric_definitions(self):
        data = []
        for m in self._metric_definitions:
            data.append(m.to_dict())
        self.write_config("metrics", data)
        self.write_config("last_change", {"ts": int(time.time())})

    def _get_metric_definitions(self):
        try:
            data = self.read_config("metrics")
        except KeyError:
            return []
        metrics = [MetricDefinition.from_dict(m) for m in data]
        return metrics

    def load_metric_definitions(self):
        m_new = self._get_metric_definitions()
        merged = merge_lists_on_key(self._metric_definitions, m_new, key=lambda x: x.id)
        self._metric_definitions = merged

    def store_event_definitions(self):
        data = []
        for e in self._event_definitions:
            data.append(e.to_dict())
        self.write_config("events", data)
        self.write_config("last_change", {"ts": int(time.time())})

    def _get_event_definitions(self):
        try:
            data = self.read_config("events")
        except KeyError:
            return []
        events = [EventDefinition.from_dict(e) for e in data]
        return events

    def load_event_definitions(self):
        e_new = self._get_event_definitions()
        merged = merge_lists_on_key(self._event_definitions, e_new, key=lambda x: x.name)
        self._event_definitions = merged

    def restore_configuration(self):
        try:
            database_init = self.read_config("database_init")
        except KeyError:
            raise RuntimeError("no database configuration. make sure this database is initialized.")
        self.load_event_definitions()
        self.load_metric_definitions()
