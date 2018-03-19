#!/usr/bin/python
# coding: utf8

import logging
import time
import msgpack
from collections import namedtuple
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud import happybase
from google.cloud.happybase.batch import Batch

from .helper import from_ts

DATA_TABLE_NAME = "timeseries"
META_TABLE_NAME = "metadata"

ALL_TABLES = [DATA_TABLE_NAME, META_TABLE_NAME]

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

    def create_tables(self):
        if self.read_only:
            raise RuntimeError("Table create in read only mode")
        i = self.get_instance(admin=True)
        tables_before = []
        for t in i.list_tables():
            tables_before.append(t.table_id)
        logger.warning(f"CREATE: Existing Tables: {tables_before}")

        tables_created = []
        for t in ALL_TABLES:
            if t in tables_before:
                continue
            table = i.table(self.table_with_prefix(t))
            table.create()
            tables_created.append(self.table_with_prefix(t))
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


            # row_key = 'greeting{}'.format(row_id)
            # row = self.get_table("cattledb_data").row(row_key)
            # row.set_cell(
            #     cf,
            #     ci,
            #     value.encode('utf-8'))
            # row.commit()
            # return row

    def read_row(self, row_id, columns=None):
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            return dt.row(row_id.encode("utf-8"), columns)

        # row_key = 'greeting{}'.format(row_id)
        # row = self.get_table("cattledb_data").read_row(row_key.encode("utf-8"), CellsColumnLimitFilter(1))
        # return row

    def reverse_day_key(self, ts):
        time_tuple = time.gmtime(ts)
        y = 5000 - int(time_tuple.tm_year)
        m = 50 - int(time_tuple.tm_mon)
        d = 50 - int(time_tuple.tm_mday)
        return "{:04d}{:02d}{:02d}".format(y,m,d)

    def insert_timeseries(self, device_key, ts):
        assert bool(ts)
        measurement = ts.key
        with self.pool.connection() as conn:
            dt = self.data_table(conn)
            b = Batch(dt, )
            for day, bucket in ts.daily_buckets():
                reverse_day_ts = self.reverse_day_key(day)
                row_key = "{}#{}".format(device_key, reverse_day_ts)
                print(row_key)
                data = {}
                for ts, v in bucket:
                    cn = "{}:{}".format(measurement, ts)
                    data[cn] = msgpack.packb(v)
                b.put(row_key, data)
            b.send()
            
                #print(row_key)
                #print(len(bucket))

                # for ts, v in day:
                # b.put
                # print(ts.key)
                # print(list(day))