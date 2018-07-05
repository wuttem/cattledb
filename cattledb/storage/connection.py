#!/usr/bin/python
# coding: utf8

import logging
import time
import os

from google.cloud import bigtable
from google.cloud import happybase
#from google.oauth2 import service_account
import google.auth


logger = logging.getLogger(__name__)


class EmulatorCreds(google.auth.credentials.Credentials):
    def refresh(self, request):
        pass


class Connection(object):
    def __init__(self, project_id, instance_id, read_only=False, pool_size=8, table_prefix="cdb",
                 credentials=None, metric_definition=None):
        self.project_id = project_id
        self.instance_id = instance_id
        self.read_only = read_only
        self.table_prefix = table_prefix
        self.credentials = credentials

        # self.credentials, project = google.auth.default()
        # credentials = service_account.Credentials.from_service_account_file('/path/to/key.json')
        bigtable_emu = os.environ.get('BIGTABLE_EMULATOR_HOST', None)
        if bigtable_emu:
            self.credentials = EmulatorCreds()

        self.client = bigtable.Client(project=self.project_id, admin=False,
                                      read_only=self.read_only, credentials=self.credentials)
        self.instance = self.client.instance(self.instance_id)
        self.admin_instance = None
        self.current_tables = None
        self.pool = happybase.ConnectionPool(pool_size, instance=self.instance)
        self.stores = {}

        self.metrics = []
        if metric_definition is not None:
            self.metrics += metric_definition

        # Register Default Data Stores
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

    def get_admin_instance(self):
        if self.read_only:
            raise RuntimeError("Cannot create admin instance in readonly mode")
        if self.admin_instance is None:
            self.admin_instance = bigtable.Client(project=self.project_id, admin=True,
                                                  credentials=self.credentials).instance(self.instance_id)
        return self.admin_instance

    def register_store(self, store):
        self.stores[store.STOREID] = store

    def get_current_tables(self, force_reload=False):
        if self.current_tables is None or force_reload:
            self.current_tables = self.get_admin_instance().list_tables()
        return self.current_tables

    def table_with_prefix(self, table_name):
        return "{}_{}".format(self.table_prefix, table_name)

    def create_tables(self, silent=False):
        for s in self.stores.values():
            s._create_tables(silent=silent)

    def create_all_metrics(self):
        self.timeseries._create_all_metrics()

    def create_metric(self, metric_name, silent=False):
        self.timeseries._create_metric(metric_name, silent=silent)


    # Table Access Methods
    def get_table(self, table_id, connection):
        return happybase.Table(self.table_with_prefix(table_id), connection)

    def timeseries_table(self, connection):
        return happybase.Table(self.table_with_prefix("timeseries"), connection)

    def metadata_table(self, connection):
        return happybase.Table(self.table_with_prefix("metadata"), connection)

    def events_table(self, connection):
        return happybase.Table(self.table_with_prefix("events"), connection)

    def counter_table(self, connection):
        return happybase.Table(self.table_with_prefix("counter"), connection)


    # Shared Methods
    def write_cell(self, table_id, row_id, column, value):
        with self.pool.connection() as conn:
            dt = happybase.Table(self.table_with_prefix(table_id), conn)
            data = {column: value.encode('utf-8')}
            dt.put(row_id, data)

    def read_row(self, table_id, row_id, columns=None):
        with self.pool.connection() as conn:
            dt = happybase.Table(self.table_with_prefix(table_id), conn)
            return dt.row(row_id.encode("utf-8"), columns)
