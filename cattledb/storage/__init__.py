#!/usr/bin/python
# coding: utf8

import logging
import time

from collections import namedtuple
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud import happybase
from google.cloud.happybase.batch import Batch

from .helper import from_ts, daily_timestamps
from .models import TimeSeries

DATA_TABLE_NAME = "timeseries"
META_TABLE_NAME = "metadata"

ALL_TABLES = [DATA_TABLE_NAME, META_TABLE_NAME]
MAX_GET_SIZE = 400 * 24 * 60 * 60  # a little bit more than a year

logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, project_id, instance_id, read_only=False, pool_size=8, table_prefix="cdb"):
        self.project_id = project_id
        self.instance_id = instance_id
        self.read_only = read_only
        self.table_prefix = table_prefix
        self.client = bigtable.Client(project=self.project_id, admin=False,
                                      read_only=self.read_only)
        self.instance = self.client.instance(self.instance_id)
        self.pool = happybase.ConnectionPool(pool_size, instance=self.instance)

    def get_instance(self, admin=False):
        return bigtable.Client(project=self.project_id, admin=admin,
                               read_only=self.read_only).instance(self.instance_id)

    def table_with_prefix(self, table_name):
        return "{}_{}".format(self.table_prefix, table_name)

    def create_tables(self, silent=False):
        if self.read_only:
            raise RuntimeError("Table create in read only mode")
        i = self.get_instance(admin=True)
        tables_before = []
        for t in i.list_tables():
            tables_before.append(t.table_id)
        logger.warning(f"CREATE: Existing Tables: {tables_before}")

        tables_created = []
        for t in ALL_TABLES:
            table_name = self.table_with_prefix(t)
            if silent and table_name in tables_before:
                continue
            table = i.table(table_name)
            table.create()
            tables_created.append(table_name)
        logger.warning(f"CREATE: Created new Tables: {tables_created}")
        return len(tables_created)

    def data_table(self, connection):
        return happybase.Table(self.table_with_prefix(DATA_TABLE_NAME), connection)

    def meta_table(self, connection):
        return happybase.Table(self.table_with_prefix(META_TABLE_NAME), connection)

    def create_data_family(self, cf, silent=False):
        if self.read_only:
            raise RuntimeError("Column Family create in read only mode")
        i = self.get_instance(admin=True)
        t = i.table(self.table_with_prefix(DATA_TABLE_NAME))
        families_before = t.list_column_families()
        logger.warning(f"CREATE CF: Existing Families: {families_before}")

        if silent and cf in families_before:
            logger.warning(f"CREATE CF: Ignoring existing family: {cf}")
            return

        cf1 = t.column_family(cf, gc_rule=MaxVersionsGCRule(1))
        cf1.create()
        logger.warning(f"CREATE CF: Created Family: {cf}")

    def write_data_cell(self, row_id, column, value):
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            data = {column: value.encode('utf-8')}
            dt.put(row_id, data)

    def read_row(self, row_id, columns=None):
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            return dt.row(row_id.encode("utf-8"), columns)

    @classmethod
    def reverse_day_key(cls, ts):
        time_tuple = time.gmtime(ts)
        y = 5000 - int(time_tuple.tm_year)
        m = 50 - int(time_tuple.tm_mon)
        d = 50 - int(time_tuple.tm_mday)
        return "{:04d}{:02d}{:02d}".format(y,m,d)

    @classmethod
    def get_row_key(cls, base_key, day_ts):
        reverse_day_ts = cls.reverse_day_key(day_ts)
        row_key = "{}#{}".format(base_key, reverse_day_ts)
        print(row_key)
        return row_key

    def insert_timeseries(self, device_id, ts):
        assert bool(ts)
        metric = ts.key
        timer = time.time()
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            b = Batch(dt, )
            for day, bucket in ts.daily_storage_buckets():
                row_key = self.get_row_key(device_id, day)
                data = {}
                for timestamp, val in bucket:
                    cn = "{}:{}".format(metric, timestamp)
                    data[cn] = val
                b.put(row_key, data)
            b.send()
        timer = time.time() - timer
        logger.info("INSERT: {}.{}, {} points in {}".format(device_id, metric, len(ts), timer))
        print("INSERT: {}.{}, {} points in {}".format(device_id, metric, len(ts), timer))
        return len(ts)
    
    def insert(self, device_id, metric, data, force_float=True):
        ts = TimeSeries(metric, data, force_float=force_float)
        return self.insert_timeseries(device_id, ts)

    def insert_bulk(self, inserts):
        out = []
        for i in inserts:
            out.append(self.insert(**i))
        return out

    @classmethod
    def dict_to_data(cls, data_dict, metrics):
        out = {m: TimeSeries(m) for m in metrics}
        for key, value in data_dict.items():
            s = key.decode("utf-8").split(":")
            if len(s) != 2:
                continue
            m = s[0]
            if m in metrics:
                ts = int(s[1])
                out[m].insert_storage_item(ts, value)
        print(out)
        return out

    def get_timeseries(self, device_id, metrics, from_ts, to_ts):
        assert from_ts <= to_ts
        assert to_ts - from_ts < MAX_GET_SIZE
        assert len(metrics) > 0
        assert len(metrics[0]) > 1
        timer = time.time()

        row_keys = [self.get_row_key(device_id, ts).encode("utf-8") for ts in daily_timestamps(from_ts, to_ts)]
        print(row_keys)

        columns = ["{}:".format(m) for m in metrics]
        print(columns)

        timeseries = {m: TimeSeries(m) for m in metrics}
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            res = dt.rows(row_keys, columns)

        for row_key, data_dict in res:
            for key, value in data_dict.items():
                s = key.decode("utf-8").split(":")
                if len(s) != 2:
                    continue
                m = s[0]
                if m in metrics:
                    ts = int(s[1])
                    timeseries[m].insert_storage_item(ts, value)

        out = []
        size = 0
        for m in metrics:
            t = timeseries[m]
            t.trim(from_ts, to_ts)
            size += len(t)
            out.append(t)

        timer = time.time() - timer
        logger.info("GET: {}.{}, {} points in {}".format(device_id, metrics, size, timer))
        print("GET: {}.{}, {} points in {}".format(device_id, metrics, size, timer))
        return out

    def get_single_timeseries(self, device_id, metric, from_ts, to_ts):
        return self.get_timeseries(device_id, [metric], from_ts, to_ts)[0]

    def get_last_values(self, device_id, metrics, count=1, max_days=365, max_ts=None):
        if max_ts is None:
            max_ts = int(time.time() + 24 * 60 * 60)

        start_search_row = self.get_row_key(device_id, max_ts).encode("utf-8")
        row_prefix = "{}#".format(device_id).encode("utf-8")
        columns = ["{}:".format(m) for m in metrics]
        print(columns)
        print(start_search_row)
        print(row_prefix)

        timer = time.time()

        timeseries = {m: TimeSeries(m) for m in metrics}

        # Start scanning
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            # with prefix
            # res = dt.scan(row_start=start_search_row, row_prefix=row_prefix, limit=max_days)
            # with row start
            res = dt.scan(row_start=start_search_row, limit=max_days)
            for row_key, data_dict  in res:
                # Break if we get another deviceid
                if not row_key.startswith(row_prefix):
                    break

                # Append to Timeseries
                print("Appending: {}".format(row_key))
                for key, value in data_dict.items():
                    s = key.decode("utf-8").split(":")
                    if len(s) != 2:
                        continue
                    m = s[0]
                    if m in metrics:
                        ts = int(s[1])
                        timeseries[m].insert_storage_item(ts, value)

                if all([len(x) >= count for x in timeseries.values()]):
                    break

        out = []
        size = 0
        for m in metrics:
            t = timeseries[m]
            t.trim_count_newest(count)
            size += len(t)
            out.append(t)
        print(out)


        timer = time.time() - timer
        logger.info("SCAN: {}.{}, {} points in {}".format(device_id, metrics, 1, timer))
        print("SCAN: {}.{}, {} points in {}".format(device_id, metrics, 1, timer))
        return out