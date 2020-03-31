#!/usr/bin/python
# coding: utf-8

import logging
import time
import struct
import json

from blinker import signal
from collections import namedtuple, defaultdict
from google.cloud import bigtable
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.column_family import MaxVersionsGCRule

from ..core.helper import (from_ts, daily_timestamps, get_metric_name_lookup, get_metric_ids,
                           get_metric_names, monthly_timestamps, get_event_name_lookup, get_metric_id_lookup)
from .models import (TimeSeries, EventList, MetaDataItem, SerializableDict,
                     ReaderActivityItem, DeviceActivityItem, RowUpsert, EventSeriesType)
from ..grpcserver.cdb_pb2 import FloatTimeSeries, FloatTimeSeriesList


logger = logging.getLogger(__name__)


class MetaDataStore(object):
    TABLENAME = "metadata"
    TABLEOPTIONS = {}
    STOREID = "metadata"

    def __init__(self, connection_object):
        self.connection_object = connection_object

    def table(self):
        return self.connection_object.get_table(self.TABLENAME)

    @classmethod
    def get_table_definitions(cls):
        return {cls.TABLENAME: ["p", "i"]}

    @classmethod
    def get_row_key(cls, object_name, object_id):
        row_key = "{}#{}".format(object_name, object_id)
        return row_key

    def put_metadata_items(self, items, internal=False):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute put_metadata in readonly mode")

        # check data
        for i in items:
            if not isinstance(i.data, dict):
                raise ValueError("Item {}.{}.{} is no dict".format(i.object_name, i.object_id, i.key))

        column = "i:" if internal else "p:"

        timer = time.time()
        row_keys = []
        upserts = []
        for i in items:
            row_key = self.get_row_key(i.object_name, i.object_id)
            row_keys.append(row_key)
            cn = "{}{}".format(column, i.key)
            data = {cn: SerializableDict(i.data).to_msgpack()}
            upserts.append(RowUpsert(row_key, data))

        dt = self.table()
        dt.upsert_rows(upserts)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(items), "row_keys": row_keys, "timer": timer, "method": "PUT"}
        sig = signal('metadata.put')
        sig.send(self, info=signal_payload)
        logger.debug("PUT META: {} inserts in {}".format(len(items), timer), extra=signal_payload)
        return len(items)

    def put_metadata(self, object_name, object_id, key, data, internal=False):
        return self.put_metadata_items([MetaDataItem(object_name, object_id, key, data)],
                                       internal=internal)

    def get_metadata(self, object_name, object_id, keys=None, internal=False):
        r = self.get_metadata_bulk(object_name, [object_id], keys=keys, internal=internal)
        if len(r) > 0:
            return r
        return None

    def get_metadata_bulk(self, object_name, object_ids, keys=None, internal=False):
        row_keys = [self.get_row_key(object_name, id) for id in object_ids]
        columns = ["i"] if internal else ["p"]

        timer = time.time()

        metadata = list()
        gen = self.table().row_generator(row_keys=row_keys, column_families=columns)

        for row_key, data_dict in gen:
            o_name, o_id = row_key.split("#")
            d = dict()
            for k, value in data_dict.items():
                s = k.split(":")
                if len(s) != 2:
                    continue
                key = s[1]
                if keys is not None and key not in keys:
                    continue
                data = SerializableDict.from_msgpack(value)
                metadata.append(MetaDataItem(o_name, o_id, key, data))

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "GET"}
        sig = signal('metadata.get')
        sig.send(self, info=signal_payload)
        logger.debug("GET METADATA: {} rows in {}".format(len(row_keys), timer), extra=signal_payload)
        return metadata


class ConfigStore(object):
    TABLENAME = "config"
    TABLEOPTIONS = {}
    STOREID = "config"
    COLUMN_FAMILY = "c"

    def __init__(self, connection_object):
        self.connection_object = connection_object

    def table(self):
        return self.connection_object.get_table(self.TABLENAME)

    @classmethod
    def get_table_definitions(cls):
        return {cls.TABLENAME: [cls.COLUMN_FAMILY]}

    def put(self, key, value):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute put config in readonly mode")

        assert len(key) > 2

        cn = "{}:value".format(self.COLUMN_FAMILY)
        row_key = key
        data = {cn: json.dumps(value).encode("ascii")}
        dt = self.table()
        dt.upsert_rows([RowUpsert(row_key, data)])

        logger.info("PUT CONFIG KEY: {}".format(key))
        return True

    def get(self, key):
        cn = "{}:value".format(self.COLUMN_FAMILY)
        row = self.table().read_row(key)  # , column_families=[self.COLUMN_FAMILY])
        raw_value = row[cn]
        logger.info("GET CONFIG KEY: {}".format(key))
        return json.loads(raw_value)


class ActivityStore(object):
    TABLENAME = "activity"
    TABLEOPTIONS = {}
    STOREID = "activity"
    MAX_GET_SIZE = 90*24*60*60
    # row: org/bs/total#reversets#reader colfam: seen:, data: hourminute_device, (device1, device2, rssi, readout_ts?)

    def __init__(self, connection_object):
        self.connection_object = connection_object

    def table(self):
        return self.connection_object.get_table(self.TABLENAME)

    @classmethod
    def get_table_definitions(cls):
        return {cls.TABLENAME: ["c"]}

    @classmethod
    def reverse_day_key(cls, ts):
        time_tuple = time.gmtime(ts)
        y = 5000 - int(time_tuple.tm_year)
        m = 50 - int(time_tuple.tm_mon)
        d = 50 - int(time_tuple.tm_mday)
        return "{:04d}{:02d}{:02d}".format(y,m,d)

    @classmethod
    def reverse_day_key_to_day(cls, reverse_key):
        y = 5000 - int(reverse_key[0:4])
        m = 50 - int(reverse_key[4:6])
        d = 50 - int(reverse_key[6:8])
        return "{:04d}{:02d}{:02d}".format(y,m,d)

    @classmethod
    def get_hour_key(cls, ts):
        time_tuple = time.gmtime(ts)
        return "{:02d}".format(time_tuple.tm_hour)

    @classmethod
    def get_row_key(cls, base_key, day_ts, reader_id=None):
        reverse_day_ts = cls.reverse_day_key(day_ts)
        row_key = "{}#{}".format(base_key, reverse_day_ts)
        if reader_id is not None:
            row_key = "{}#{}".format(row_key, reader_id)
        return row_key

    @classmethod
    def get_insert_keys(cls, reader_id, day_ts, parent_ids=None):
        assert 3 <= len(reader_id) <= 32
        reverse_day_ts = cls.reverse_day_key(day_ts)
        total_key = "t#{}#{}".format(reverse_day_ts, reader_id)
        row_keys = [total_key]
        if parent_ids is not None:
            assert 1 <= len(parent_ids) <= 3
            for p in parent_ids:
                assert 3 <= len(p) <= 32
                row_keys.append("{}#{}#{}".format(p, reverse_day_ts, reader_id))
        return row_keys

    def incr_activity(self, reader_id, device_id, timestamp, parent_ids=None, value=1):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute incr_activity in readonly mode")

        if not (time.time() - 3*365*24*60*60) < timestamp < (time.time() + 30*24*60*60):
            raise ValueError("timestamp out of activity window -3y +30d")

        row_keys = self.get_insert_keys(reader_id, timestamp, parent_ids)
        column = "c:{}.{}".format(self.get_hour_key(timestamp), device_id)

        timer = time.time()
        res = []
        table = self.table()
        for r in row_keys:
            res.append(table.increment_counter(r, column, value))

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "PUT"}
        sig = signal('activity.incr')
        sig.send(self, info=signal_payload)
        logger.debug("INCR ACTIVITY: {}, {} incrs in {}".format(device_id, len(res), timer), extra=signal_payload)
        # print("INCR ACTIVITY: {}, {} incrs in {}".format(device_id, len(res), timer))
        return res

    def get_total_activity_for_day(self, day_ts):
        return self.get_activity_for_day("t", day_ts)

    def get_activity_for_reader(self, reader_id, from_ts, to_ts):
        assert from_ts <= to_ts
        assert to_ts - from_ts < self.MAX_GET_SIZE

        daily_ts =  daily_timestamps(from_ts, to_ts)
        row_keys = [self.get_row_key("t", ts, reader_id=reader_id) for ts in daily_ts]
        columns = ["c"]

        timer = time.time()

        activitys = defaultdict(lambda: defaultdict(int))
        rowgen = self.table().row_generator(row_keys=row_keys,
                                            column_families=columns)

        for row_key, data_dict in rowgen:
            day = self.reverse_day_key_to_day(row_key.split("#")[-2])
            # Append to Activity
            for k, value in data_dict.items():
                s = k.split(":")
                if len(s) != 2:
                    continue
                p = s[1].split(".")
                if len(p) != 2:
                    continue
                day_hour = "{}{}".format(day, p[0])
                # parse value
                int_value, = struct.Struct('>q').unpack(value)
                activitys[day_hour][p[1]] += int_value

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "GET"}
        sig = signal('activity.get')
        sig.send(self, info=signal_payload)
        logger.debug("GET ACTIVITY: {} rows in {}".format(len(row_keys), timer), extra=signal_payload)
        # print("GET ACTIVITY: {} rows in {}".format(len(row_keys), timer))
        out = []
        for day_hour in sorted(activitys.keys()):
            inner = activitys[day_hour]
            for device_id in sorted(inner.keys()):
                count = inner[device_id]
                out.append(DeviceActivityItem(day_hour, device_id, count))
        return out

    def get_activity_for_day(self, parent_id, day_ts):
        row_start_search = self.get_row_key(parent_id, day_ts)
        columns = ["c"]

        timer = time.time()

        activitys = defaultdict(lambda: defaultdict(list))
        row_counter = 0
        row_keys = []

        # Start scanning
        row_gen = self.table().row_generator(start_key=row_start_search, column_families=columns,
                                             check_prefix=row_start_search)

        for row_key, data_dict in row_gen:
            readout_id = row_key.split("#")[-1]
            row_keys.append(row_key)
            day = self.reverse_day_key_to_day(row_key.split("#")[-2])

            for k, value in data_dict.items():
                s = k.split(":")
                if len(s) != 2:
                    continue
                p = s[1].split(".")
                if len(p) != 2:
                    continue
                day_hour = "{}{}".format(day, p[0])
                activitys[day_hour][readout_id].append(p[1])

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "SCAN"}
        sig = signal('activity.get')
        sig.send(self, info=signal_payload)
        logger.debug("SCAN ACTIVITY: {} rows in {}".format(1, timer), extra=signal_payload)
        # print("SCAN ACTIVITY: {} rows in {}".format(1, timer))
        out = []
        for day_hour in sorted(activitys.keys()):
            inner = activitys[day_hour]
            for reader_id in sorted(inner.keys()):
                devices = inner[reader_id]
                out.append(ReaderActivityItem(day_hour, reader_id, devices))
        return out


class TimeSeriesStore(object):
    TABLENAME = "timeseries"
    TABLEOPTIONS = {}
    STOREID = "timeseries"
    MAX_GET_SIZE = 400 * 24 * 60 * 60  # A bit more than a year

    def __init__(self, connection_object):
        self.connection_object = connection_object

    def table(self):
        return self.connection_object.get_table(self.TABLENAME)

    @property
    def METRIC_NAME_LOOKUP(self):
        return get_metric_name_lookup(self.connection_object.metric_definitions)

    @property
    def METRIC_NAMES(self):
        return get_metric_names(self.connection_object.metric_definitions)

    @property
    def METRIC_IDS(self):
        return get_metric_ids(self.connection_object.metric_definitions)

    @property
    def METRIC_ID_LOOKUP(self):
        return get_metric_id_lookup(self.connection_object.metric_definitions)

    @classmethod
    def get_table_definitions(cls):
        return {cls.TABLENAME: ["_meta", "_v"]}

    def _create_metric(self, metric_name, silent=False):
        # todo: deprecate this method
        return self.connection_object.create_metric(metric_name, silent=silent)

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
        return row_key

    def get_metric_object(self, metric_name):
        if metric_name in self.METRIC_NAMES:
            return self.METRIC_NAME_LOOKUP[metric_name]
        raise KeyError("metric {} not known".format(metric_name))

    def insert_timeseries(self, ts):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute insert_timeseries command in readonly mode")

        assert bool(ts)
        metric_object = self.get_metric_object(ts.metric)
        row_keys = []
        key = ts.key
        timer = time.time()

        row_keys = []
        upserts = []
        for day, bucket in ts.daily_storage_buckets():
            row_key = self.get_row_key(key, day)
            row_keys.append(row_key)
            data = {}
            for timestamp, val in bucket:
                cn = "{}:{}".format(metric_object.id, timestamp)
                data[cn] = val
            upserts.append(RowUpsert(row_key, data))

        dt = self.table()
        dt.upsert_rows(upserts)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "PUT"}
        sig = signal('timeseries.put')
        sig.send(self, info=signal_payload)
        logger.debug("INSERT: {}.{}, {} points in {}".format(key, metric_object.name, len(ts), timer), extra=signal_payload)
        # print("INSERT: {}.{}, {} points in {}".format(key, metric, len(ts), timer))
        return len(ts)

    def insert(self, key, metric, data):
        ts = TimeSeries(key, metric, data)
        return self.insert_timeseries(ts)

    def insert_bulk(self, inserts):
        out = []
        for i in inserts:
            out.append(self.insert(**i))
        return out

    def get_timeseries(self, key, metrics, from_ts, to_ts):
        assert from_ts <= to_ts
        assert to_ts - from_ts < self.MAX_GET_SIZE
        assert len(metrics) > 0
        assert len(metrics[0]) > 1
        timer = time.time()

        metric_objects = [self.get_metric_object(m) for m in metrics]

        row_keys = [self.get_row_key(key, ts) for ts in daily_timestamps(from_ts, to_ts)]
        first_key = self.get_row_key(key, to_ts)
        last_key = self.get_row_key(key, from_ts)
        columns = ["{}".format(m.id) for m in metric_objects]

        timeseries = {m.id: TimeSeries(key, m.name) for m in metric_objects}
        #res = self.table().read_rows(row_keys=row_keys, column_families=columns)
        #gen = self.table().row_generator(row_keys=row_keys, column_families=columns)
        gen = self.table().row_generator(start_key=first_key, end_key=last_key, column_families=columns)

        for row_key, data_dict in gen:
            for k in reversed(data_dict):
                s = k.split(":")
                if len(s) != 2:
                    continue
                m = s[0]
                if m in timeseries.keys():
                    ts = int(s[1])
                    timeseries[m].insert_storage_item(ts, data_dict[k])

        out = []
        size = 0
        for m in metric_objects:
            t = timeseries[m.id]
            t.trim(from_ts, to_ts)
            size += len(t)
            out.append(t)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "GET"}
        sig = signal('timeseries.get')
        sig.send(self, info=signal_payload)
        logger.debug("GET: {}.{}, {} points in {}".format(key, metrics, size, timer), extra=signal_payload)
        # print("GET: {}.{}, {} points in {}".format(key, metrics, size, timer))
        return out

    def get_single_timeseries(self, key, metric, from_ts, to_ts):
        return self.get_timeseries(key, [metric], from_ts, to_ts)[0]

    def get_last_value(self, key, metric, min_ts=None, max_ts=None):
        """searches for the newest value for a given metric.
        min_ts gives the minimum to search.
        """
        if max_ts is not None:
            start_search_row = self.get_row_key(key, max_ts)
        else:
            start_search_row = "{}#".format(key)

        if min_ts is not None:
            end_search_row = self.get_row_key(key, min_ts)
        else:
            end_search_row = "{}+".format(key)

        metric_object = self.get_metric_object(metric)
        columns = [metric_object.id]

        timer = time.time()
        row_keys = []

        series = TimeSeries(key, metric_object.name)
        # Start scanning
        row = self.table().get_first_row(start_search_row, column_families=columns, end_key=end_search_row)
        if row is not None:
            row_key, data_dict = row
            row_keys.append(row_key)

            # Append to Timeseries
            for k, value in data_dict.items():
                s = k.split(":")
                if len(s) != 2:
                    continue
                m = s[0]
                if m != metric_object.id:
                    raise ValueError("wrong metric in database {} != {}".format(m, metric_object.id))
                ts = int(s[1])
                series.insert_storage_item(ts, value)

        series.trim_count_newest(1)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "SCAN"}
        sig = signal('timeseries.last')
        sig.send(self, info=signal_payload)
        logger.debug("SCAN: {}.{}, {} points in {}".format(key, metric, 1, timer), extra=signal_payload)
        # print("SCAN: {}.{}, {} points in {}".format(key, metric, 1, timer))
        return series

    def get_full_timeseries(self, key):
        return self.get_all_metrics(key, from_ts=None, to_ts=None)

    def get_all_metrics(self, key, from_ts, to_ts):
        if from_ts is not None and to_ts is not None:
            assert from_ts <= to_ts

        if to_ts is not None:
            start_search_row = self.get_row_key(key, to_ts)
        else:
            start_search_row = "{}#".format(key)

        if from_ts is not None:
            end_search_row = self.get_row_key(key, from_ts)
        else:
            end_search_row = "{}+".format(key)

        timer = time.time()
        row_keys = []

        # Start scanning
        row_gen = self.table().row_generator(start_key=start_search_row, end_key=end_search_row,
                                             column_families=None)
        

        timeseries = defaultdict(lambda: TimeSeries(key, "_unknown"))
        for row_key, data_dict in row_gen:
            for k in reversed(data_dict):
                s = k.split(":")
                if len(s) != 2:
                    continue
                metric_id = s[0]
                timestamp = int(s[1])
                # reverse lookup
                _all_ids = self.METRIC_ID_LOOKUP
                if metric_id in _all_ids:
                    metric_name = _all_ids[metric_id].name
                else:
                    metric_name = metric_id
                timeseries[metric_name].insert_storage_item(timestamp, data_dict[k])

        size = 0
        for name, ts in timeseries.items():
            ts.set_metric(name)
            if len(ts) < 1:
                continue
            if from_ts is not None and to_ts is not None:
                ts.trim(from_ts, to_ts)
            elif from_ts is not None:
                ts.trim(from_ts, ts.ts_max)
            elif to_ts is not None:
                ts.trim(ts.ts_min, to_ts)
            size += len(ts)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "timer": timer, "method": "GET"}
        sig = signal('timeseries.full')
        sig.send(self, info=signal_payload)
        logger.debug("FULL: {}, {} points in {}".format(key, size, timer), extra=signal_payload)
        return list(timeseries.values())

    def get_last_values(self, key, metrics):
        return [self.get_last_value(key, m) for m in metrics]

    def delete_timeseries(self, key, metrics, from_ts, to_ts):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute delete_timeseries command in readonly mode")

        assert from_ts <= to_ts
        assert len(metrics) > 0
        assert len(metrics[0]) > 1
        timer = time.time()

        row_keys = [self.get_row_key(key, ts) for ts in daily_timestamps(from_ts, to_ts)]
        metric_objects = [self.get_metric_object(m) for m in metrics]
        # Check for delete flag
        columns = []
        for m in metric_objects:
            if not m.delete_possible:
                raise RuntimeError("Delete not possible on metric {}".format(m.name))
            columns.append("{}".format(m.id))

        table = self.table()
        for row_key in row_keys:
            table.delete_row(row_key, column_families=columns)

        timer = time.time() - timer
        count = len(row_keys)
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "DELETE"}
        sig = signal('timeseries.delete')
        sig.send(self, info=signal_payload)
        logger.debug("DELETE: {}.{}, {} days in {}".format(key, metrics, count, timer), extra=signal_payload)
        # print("DELETE: {}.{}, {} days in {}".format(key, metrics, count, timer))
        return count

class EventStore(object):
    """
    Event Store.

    For storing variable Timeseries Data.
    There is a store with daily and a store with monthly rows.
    For daily rows one event every second is possible.
    For monthly rows one event every minute is possible.
    """
    TABLENAME = "events"
    TABLEOPTIONS = {}
    STOREID = "events"
    MAX_GET_SIZE_DAILY = 45 * 24 * 60 * 60
    MAY_GET_SIZE_MONTHLY = 4 * 365 * 24 * 60 * 60
    DEFAULT_SERIES_TYPE = EventSeriesType.DAILY

    def __init__(self, connection_object):
        self.connection_object = connection_object

    @property
    def EVENTS(self):
        return self.connection_object.event_definitions

    def table(self):
        return self.connection_object.get_table(self.TABLENAME)

    @classmethod
    def get_table_definitions(cls):
        return {cls.TABLENAME: ["e"]}

    def get_type_for_name(self, name):
        for ev_def in self.EVENTS:
            if ((ev_def.name[-1] == "*" and name.startswith(ev_def.name[:-1]))
                or ev_def.name == name):
                return EventSeriesType(ev_def.type.value)
        return self.DEFAULT_SERIES_TYPE

    @classmethod
    def reverse_day_key(cls, ts):
        time_tuple = time.gmtime(ts)
        y = 5000 - int(time_tuple.tm_year)
        m = 50 - int(time_tuple.tm_mon)
        d = 50 - int(time_tuple.tm_mday)
        return "{:04d}{:02d}{:02d}".format(y, m, d)

    @classmethod
    def reverse_month_key(cls, ts):
        time_tuple = time.gmtime(ts)
        y = 5000 - int(time_tuple.tm_year)
        m = 50 - int(time_tuple.tm_mon)
        return "{:04d}{:02d}".format(y, m)

    def get_row_key(self, base_key, name, day_ts):
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            row_key = "{}#{}#{}".format(base_key, name, self.reverse_day_key(day_ts))
        elif t == EventSeriesType.MONTHLY:
            row_key = "{}#m_{}#{}".format(base_key, name, self.reverse_month_key(day_ts))
        else:
            raise ValueError("invalid EventSeriesType")
        return row_key

    def get_row_key_base(self, base_key, name):
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            row_key = "{}#{}#".format(base_key, name)
        elif t == EventSeriesType.MONTHLY:
            row_key = "{}#m_{}#".format(base_key, name)
        else:
            raise ValueError("invalid EventSeriesType")
        return row_key

    def insert_events(self, event_list):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute insert_eventlist command in readonly mode")

        assert bool(event_list)
        name = event_list.metric
        key = event_list.key

        timer = time.time()
        row_keys = []
        upserts = []

        # Monthly or Daily
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            it = event_list.daily_storage_buckets()
        elif t == EventSeriesType.MONTHLY:
            it = event_list.monthly_storage_buckets()
        else:
            raise ValueError("invalid EventSeriesType")

        for ts, bucket in it:
            row_key = self.get_row_key(key, name, ts)
            row_keys.append(row_key)
            data = {}
            for timestamp, val in bucket:
                cn = "e:{}".format(timestamp)
                data[cn] = val
            upserts.append(RowUpsert(row_key, data))

        dt = self.table()
        dt.upsert_rows(upserts)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "PUT"}
        sig = signal('event.put')
        sig.send(self, info=signal_payload)
        logger.debug("INSERT EVENTS: {}.{}, {} points in {}".format(key, name, len(event_list), timer), extra=signal_payload)
        # print("INSERT EVENTS: {}.{}, {} points in {}".format(key, name, len(event_list), timer))
        return len(event_list)

    def insert_event(self, key, name, dt, data):
        return self.insert_events(EventList(key, name, [(dt, data)]))

    def max_get_size(self, name):
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            return self.MAX_GET_SIZE_DAILY
        elif t == EventSeriesType.MONTHLY:
            return self.MAY_GET_SIZE_MONTHLY
        else:
            raise ValueError("invalid EventSeriesType")

    def get_events(self, key, name, from_ts, to_ts):
        assert from_ts <= to_ts
        assert to_ts - from_ts < self.max_get_size(name)
        timer = time.time()

        # Monthly or Daily
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            it = daily_timestamps(from_ts, to_ts)
        elif t == EventSeriesType.MONTHLY:
            it = monthly_timestamps(from_ts, to_ts)
        else:
            raise ValueError("invalid EventSeriesType")

        row_keys = [self.get_row_key(key, name, ts) for ts in it]
        columns = ["e"]

        #res = self.table().read_rows(row_keys=row_keys, column_families=columns)
        gen = self.table().row_generator(row_keys=row_keys, column_families=columns)

        events = EventList(key, name)
        for row_key, data_dict in gen:
            for k, value in data_dict.items():
                s = k.split(":")
                if len(s) != 2:
                    continue
                m = s[0]
                ts = int(s[1])
                events.insert_storage_item(ts, value)

        events.trim(from_ts, to_ts)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "GET"}
        sig = signal('event.get')
        sig.send(self, info=signal_payload)
        logger.debug("GET EVENTS: {}.{}, {} points in {}".format(key, name, len(events), timer), extra=signal_payload)
        # print("GET EVENTS: {}.{}, {} points in {}".format(key, name, len(events), timer))
        return events

    def get_last_event(self, key, name):
        return self.get_last_events(key, name, count=1)

    def get_last_events(self, key, name, count=1, min_ts=None, max_ts=None):
        assert count == 1

        if max_ts is not None:
            start_search_row = self.get_row_key(key, name, max_ts)
        else:
            start_search_row = self.get_row_key_base(key, name)

        if min_ts is not None:
            end_search_row = self.get_row_key(key, name, min_ts)
        else:
            end_search_row = self.get_row_key_base(key, name)[:-1] + "+"

        columns = ["e"]

        timer = time.time()
        row_keys = []

        events = EventList(key, name)
        # Start scanning
        row = self.table().get_first_row(start_search_row, column_families=columns, end_key=end_search_row)
        if row is not None:
            row_key, data_dict = row
            row_keys.append(row_key)

            # Append to Timeseries
            for key, value in data_dict.items():
                s = key.split(":")
                if len(s) != 2:
                    continue
                m = s[0]
                ts = int(s[1])
                events.insert_storage_item(ts, value)

        events.trim_count_newest(1)

        timer = time.time() - timer
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "SCAN"}
        sig = signal('event.last')
        sig.send(self, info=signal_payload)
        logger.debug("SCAN EVENTS: {}.{}, {} points in {}".format(key, name, len(events), timer), extra=signal_payload)
        # print("SCAN EVENTS: {}.{}, {} points in {}".format(key, name, len(events), timer))
        return events

    def delete_event_days(self, key, name, from_ts, to_ts):
        if self.connection_object.read_only:
            raise RuntimeError("Cannot execute delete_event_days command in readonly mode")

        assert from_ts <= to_ts
        timer = time.time()

        # Monthly or Daily
        t = self.get_type_for_name(name)
        if t == EventSeriesType.DAILY:
            it = daily_timestamps(from_ts, to_ts)
        elif t == EventSeriesType.MONTHLY:
            it = monthly_timestamps(from_ts, to_ts)
        else:
            raise ValueError("invalid EventSeriesType")

        row_keys = [self.get_row_key(key, name, ts) for ts in it]

        table = self.table()
        for row_key in row_keys:
            table.delete_row(row_key)

        timer = time.time() - timer
        count = len(row_keys)
        # emit signal
        signal_payload = {"count": len(row_keys), "row_keys": row_keys, "timer": timer, "method": "DELETE"}
        sig = signal('event.delete')
        sig.send(self, info=signal_payload)
        logger.debug("DELETE EVENTS: {}.{}, {} days in {}".format(key, name, count, timer), extra=signal_payload)
        # print("DELETE EVENTS: {}.{}, {} days in {}".format(key, name, count, timer))
        return count
