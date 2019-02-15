#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals
from builtins import str

import sys
from enum import Enum
from collections import MutableSequence, namedtuple, deque
import itertools
import pendulum
from datetime import datetime
import msgpack
import struct
import json
import six

import bisect
import logging
import array
import hashlib

from ..grpcserver.cdb_pb2 import FloatTimeSeries, Dictionary, DictTimeSeries, Pair, MetaDataDict, ReaderActivity, DeviceActivity

from .helper import ts_daily_left, ts_daily_right
from .helper import ts_hourly_left, ts_hourly_right
from .helper import ts_weekly_left, ts_weekly_right
from .helper import ts_monthly_left, ts_monthly_right


class _sliceable_deque(deque):
    def __getitem__(self, index):
        try:
            return deque.__getitem__(self, index)
        except TypeError:
            start = index.start
            if index.start is not None and index.start < 0:
                start = len(self) + index.start

            stop = index.stop
            if index.stop is not None and index.stop < 0:
                stop = len(self) + index.stop

            if index.step is not None and index.step < 0:
                if index.start is not None or index.stop is not None:
                    raise ValueError("reverse iteration on slice is not possible")
                step = abs(index.step)
                sli = itertools.islice(self, start, stop, step)
                return type(self)(reversed(type(self)(sli)))

            sli = itertools.islice(self, start, stop, index.step)
            return type(self)(sli)

class _list(list):
    def appendleft(self, item):
        return self.insert(0, item)

if six.PY2:
    sliceable_deque = _list
else:
    sliceable_deque = _sliceable_deque


Point = namedtuple('Point', ['ts', 'value', 'dt'])
RawPoint = namedtuple('RawPoint', ['ts', 'value', 'ts_offset'])
MetaDataItem = namedtuple('MetaDataItem', ["object_name", "object_id", "key", "data"])

TimestampWithOffset = namedtuple('TimestampWithOffset', ["ts", "offset"])
RowUpsert = namedtuple('RowUpsert', ['row_key', 'cells'])


class SeriesType(Enum):
    FLOATSERIES = 1
    DICTSERIES = 2


class EventSeriesType(Enum):
    DAILY = 1
    MONTHLY = 2


class TimeSeriesKeyWrapper:
    key_getter = lambda c: c.ts

    def __init__(self, timeseries):
        self.timeseries = timeseries

    def __getitem__(self, i):
        return self.timeseries._data[i].ts

    def __len__(self):
        return len(self.timeseries)


class TimeSeries(object):
    DEFAULT_TYPE = SeriesType.FLOATSERIES
    TYPE_WRAPPER = Point

    def __init__(self, key, metric, values=None, series_type=None):
        self._data = sliceable_deque()
        if series_type is None:
            self.series_type = self.DEFAULT_TYPE
        else:
            self.series_type = series_type
        if values is not None:
            self.insert(values)
        self.key = key.lower()
        self.metric = metric.lower()
        assert len(self.key) >= 2
        assert len(self.metric) >= 2

    @classmethod
    def from_proto_bytes(cls, b, series_type=None):
        if series_type is None:
            series_type = cls.DEFAULT_TYPE
        if series_type == SeriesType.FLOATSERIES:
            f = FloatTimeSeries()
        elif series_type == SeriesType.DICTSERIES:
            f = DictTimeSeries()
        else:
            raise NotImplementedError("wrong series type")
        f.ParseFromString(b)
        return cls.from_proto(f, series_type=series_type)

    @classmethod
    def from_proto(cls, p, series_type=None):
        if series_type is None:
            series_type = cls.DEFAULT_TYPE
        i = cls(p.key, p.metric, series_type=series_type)
        i._data = sliceable_deque(map(RawPoint._make, 
                                      zip(
                                          p.timestamps,
                                          p.values,
                                          p.timestamp_offsets)))
        i.check_series()
        return i

    @classmethod
    def from_list(cls, key, metric, values):
        return cls(key, metric, values)

    def __len__(self):
        return len(self._data)

    def empty(self):
        if len(self) < 1:
            return True
        return False

    def check_series(self):
        if len(self) > 0:
            self.check_sorted()

    def __bool__(self):  # Python 3
        self.check_series()
        if len(self) > 0:
            return True
        return False

    def check_sorted(self):
        it = iter(self._data)
        if (sys.version_info > (3, 0)):
            it.__next__()
        else:
            it.next()
        assert all(b.ts >= a.ts for a, b in zip(self._data, it))

    def to_hash(self):
        s = "{}.{}.{}.{}.{}".format(self.key, self.metric, len(self),
                                    self.ts_min, self.ts_max)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, TimeSeries):
            return False
        # Is Hashing a Performance Problem ?
        h1 = self.to_hash()
        h2 = other.to_hash()
        return h1 == h2

    def append_timeseries(self, other):
        if not isinstance(other, TimeSeries):
            raise ValueError("cannot append %s to TimeSeries", other)

        other.check_series()

        if len(other) < 1:
            return

        assert self.ts_max < other.ts_min
        assert self.key == other.key
        assert self.metric == other.metric
        assert self.series_type == other.series_type

        self._data += other._data

    def __ne__(self, other):
        return not self == other  # NOT return not self.__eq__(other)

    def __repr__(self):
        l = len(self._data)
        if l > 0:
            m = self._data[0].ts
        else:
            m = -1
        return "<{}.{} series({}), min_ts: {}>".format(
            self.key, self.metric, l, m)

    @property
    def ts_max(self):
        if len(self._data) > 0:
            return self._data[-1].ts
        return -1

    @property
    def ts_min(self):
        if len(self._data) > 0:
            return self._data[0].ts
        return -1

    @property
    def first(self):
        return None if self.empty() else self[0]

    @property
    def last(self):
        return None if self.empty() else self[-1]

    @property
    def count(self):
        return len(self._data)

    def _at(self, i, raw=False):
        if raw:
            return self._data[i]
        dt = pendulum.from_timestamp(self._data[i].ts, self._data[i].ts_offset/3600.0)
        return self.TYPE_WRAPPER(self._data[i].ts, self._data[i].value, dt)

    def _storage_item_at(self, i):
        if self.series_type == SeriesType.FLOATSERIES:
            by = struct.pack("B", 1) + struct.pack("i", self._data[i].ts_offset) + struct.pack("f", self._data[i].value)
        elif self.series_type == SeriesType.DICTSERIES:
            by = struct.pack("B", 2) + struct.pack("i", self._data[i].ts_offset) + msgpack.packb(self._data[i].value, use_bin_type=True)
        else:
            raise NotImplementedError("wrong series type")
        return (self._data[i].ts, by)

    def _serializable_at(self, i):
        dt = pendulum.from_timestamp(self._data[i].ts, self._data[i].ts_offset/3600.0)
        return (dt.isoformat(), self._data[i].value)

    def __getitem__(self, key):
        return self._at(key)

    def to_list(self):
        out = list()
        for i in range(len(self._data)):
            out.append(self._at(i))
        return out

    def ts_iterator(self):
        return TimeSeriesKeyWrapper(self)

    def bisect_left(self, timestamp):
        if len(self._data) < 1 or timestamp < self.ts_min:
            return 0
        elif timestamp > self.ts_max:
            return len(self._data)
        return bisect.bisect_left(self.ts_iterator(), timestamp)

    def bisect_right(self, timestamp):
        if len(self._data) < 1 or timestamp < self.ts_min:
            return 0
        elif timestamp > self.ts_max:
            return len(self._data)
        return bisect.bisect_right(self.ts_iterator(), timestamp)

    def insert_storage_item(self, timestamp, by, overwrite=False):
        f = int(struct.unpack("B", by[0:1])[0])
        offset = int(struct.unpack("i", by[1:5])[0])

        if f == 1 and self.series_type == SeriesType.FLOATSERIES:
            value = float(struct.unpack("f", by[5:9])[0])
        elif f == 2 and self.series_type == SeriesType.DICTSERIES:
            value = msgpack.unpackb(by[5:], raw=False)
        else:
            raise RuntimeError("Invalid series type or type miss match")

        idx = self.bisect_left(timestamp)

        # Prepend
        if idx == 0:
            self._data.appendleft(RawPoint(timestamp, value, offset))
            return 1
        # Append
        if idx == len(self._data):
            self._data.append(RawPoint(timestamp, value, offset))
            return 1
        # Already Existing
        if self._data[idx].ts == timestamp:
            # Replace
            logging.debug("duplicate insert")
            if overwrite:
                self._data[idx] = RawPoint(timestamp, value, offset)
                return 1
            return 0
        # Insert
        self._data.insert(idx, RawPoint(timestamp, value, offset))
        return 1

    def insert_point(self, dt, value, overwrite=False):
        if isinstance(dt, int):
            timestamp = dt
            offset = 0
        elif isinstance(dt, float):
            timestamp = int(dt)
            offset = 0
        elif isinstance(dt, TimestampWithOffset):
            timestamp = int(dt.ts)
            offset = int(dt.offset)
        elif isinstance(dt, pendulum.DateTime):
            timestamp = dt.int_timestamp
            offset = dt.offset
        elif isinstance(dt, datetime):
            pd = pendulum.instance(dt)
            timestamp = pd.int_timestamp
            offset = pd.offset
        elif isinstance(dt, tuple):
            timestamp = int(dt[0])
            offset = int(dt[1])
        else:
            raise ValueError("Invalid TS format: %s", dt)

        idx = self.bisect_left(timestamp)

        # Force Float
        if self.series_type == SeriesType.FLOATSERIES:
            value = float(value)
        # Force Dict
        if self.series_type == SeriesType.DICTSERIES:
            value = dict(value)

        # Append
        if idx == len(self._data):
            self._data.append(RawPoint(timestamp, value, offset))
            return 1
        # Already Existing
        if self._data[idx].ts == timestamp:
            # Replace
            logging.debug("duplicate insert")
            if overwrite:
                self._data[idx] = RawPoint(timestamp, value, offset)
                return 1
            return 0
        # Insert
        self._data.insert(idx, RawPoint(timestamp, value, offset))
        return 1

    def insert(self, series):
        counter = 0
        for timestamp, value in series:
            counter += self.insert_point(timestamp, value)
        self.check_series() # may be removed
        return counter

    def get_index_below_ts(self, ts):
        idx = self.bisect_left(ts) - 1
        if idx >= len(self._data):
            return None
        if idx >= 0:
            return idx
        return None

    def trim(self, ts_min, ts_max):
        low = self.bisect_left(ts_min)
        high = self.bisect_right(ts_max)
        self._data = self._data[low:high]

    def trim_count_newest(self, count):
        if len(self) <= count:
            return
        self._data = self._data[-int(count):]

    def trim_count_oldest(self, count):
        if len(self) <= count:
            return
        self._data = self._data[:int(count)]

    def all(self, raw=False):
        """Return an iterator to get all ts value pairs.
        """
        i = 0
        while i < len(self._data):
            yield self._at(i, raw=raw)
            i += 1

    def yield_range(self, ts_min, ts_max, raw=False):
        """Return an iterator to get all ts value pairs in range.
        """
        low = self.bisect_left(ts_min)
        high = self.bisect_right(ts_max)

        i = low
        while i < high:
            yield self._at(i, raw=raw)
            i += 1

    def daily(self, raw=False):
        """Generator to access daily data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._data):
            j = 0
            lower_bound = ts_daily_left(self._data[i].ts)
            upper_bound = ts_daily_right(self._data[i].ts)
            while (i + j < len(self._data) and
                   lower_bound <= self._data[i + j].ts <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def monthly_storage_buckets(self):
        i = 0
        while i < len(self._data):
            j = 0
            lower_bound = ts_monthly_left(self._data[i].ts)
            upper_bound = ts_monthly_right(self._data[i].ts)
            while (i + j < len(self._data) and
                   lower_bound <= self._data[i + j].ts <= upper_bound):
                j += 1
            yield (lower_bound, [self._storage_item_at(x) for x in range(i, i + j)])
            i += j

    def daily_storage_buckets(self):
        i = 0
        while i < len(self._data):
            j = 0
            lower_bound = ts_daily_left(self._data[i].ts)
            upper_bound = ts_daily_right(self._data[i].ts)
            while (i + j < len(self._data) and
                   lower_bound <= self._data[i + j].ts <= upper_bound):
                j += 1
            yield (lower_bound, [self._storage_item_at(x) for x in range(i, i + j)])
            i += j

    def to_lists(self):
        if len(self) > 0:
            timestamps, values, timestamp_offsets = zip(*self._data)
        else:
            timestamps, values, timestamp_offsets = [], [], []
        return timestamps, values, timestamp_offsets

    def to_proto(self):
        print(self._data)
        timestamps, values, timestamp_offsets = self.to_lists()

        if self.series_type == SeriesType.FLOATSERIES:
            ts = FloatTimeSeries()
            ts.values.extend(values)
        elif self.series_type == SeriesType.DICTSERIES:
            ts = DictTimeSeries()
            proto_dicts = []
            for v in values:
                proto_dicts.append(SerializableDict(v).to_proto())
            ts.values.extend(proto_dicts)
        else:
            raise NotImplementedError("wrong series type")
        ts.metric = self.metric
        ts.key = self.key
        ts.timestamps.extend(timestamps)
        ts.timestamp_offsets.extend(timestamp_offsets)
        return ts

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def to_serializable(self):
        i = 0
        while i < len(self._data):
            yield self._serializable_at(i)
            i += 1

    def aligned_10minute(self, raw=False):
        """Generator to data aligned to 10min period.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._data):
            j = 0
            lower_bound = self._data[i].ts - (self._data[i].ts % (10*60))
            upper_bound = lower_bound + 10*60 - 1
            while (i + j < len(self._data) and
                   lower_bound <= self._data[i + j].ts <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def hourly(self, raw=False):
        """Generator to access hourly data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._data):
            j = 0
            lower_bound = ts_hourly_left(self._data[i].ts)
            upper_bound = ts_hourly_right(self._data[i].ts)
            while (i + j < len(self._data) and
                   lower_bound <= self._data[i + j].ts <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def aggregation(self, group="hourly", function="mean", raw=False):
        """Aggregation Generator.
        """
        if group == "hourly":
            it = self.hourly
            left = ts_hourly_left
        elif group == "daily":
            it = self.daily
            left = ts_daily_left
        elif group == "10min":
            it = self.aligned_10minute
            left = lambda x: x - (x % (10*60))
        else:
            raise ValueError("Invalid aggregation group")

        if function == "sum":
            func = sum
        elif function == "count":
            func = len
        elif function == "min":
            func = min
        elif function == "max":
            func = max
        elif function == "amp":
            def amp(x):
                return max(x) - min(x)
            func = amp
        elif function == "mean":
            def mean(x):
                if len(x) == 1:
                    return x[0]
                return sum(x) / len(x)
            func = mean
        else:
            raise ValueError("Invalid aggregation group")

        if raw:
            for g in it(raw=True):
                t = list(g)
                ts = left(t[0].ts)
                offset = t[0].ts_offset
                value = func([x.value for x in t])
                yield RawPoint(ts, value, offset)
        else:
            for g in it(raw=True):
                t = list(g)
                ts = left(t[0].ts)
                offset = t[0].ts_offset
                #offset = t[0].dt.offset
                dt = pendulum.from_timestamp(ts, offset/3600.0)
                value = func([x.value for x in t])
                yield self.TYPE_WRAPPER(ts, value, dt)


class EventList(TimeSeries):
    DEFAULT_TYPE = SeriesType.DICTSERIES

    def __init__(self, key, name, events=None):
        super(EventList, self).__init__(key=key, metric=name, values=events,
                                        series_type=SeriesType.DICTSERIES)

    @property
    def name(self):
        return self.metric

    @classmethod
    def from_proto(cls, p):
        i = cls(p.key, p.name)
        i._data = sliceable_deque(map(RawPoint._make, 
                                      zip(
                                          p.timestamps,
                                          [SerializableDict.from_proto(x) for x in p.values],
                                          p.timestamp_offsets)))
        i.check_series()
        return i


class SerializableDict(dict):
    def __init__(self, *args, **kwargs):
        super(SerializableDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    @classmethod
    def from_proto_bytes(cls, b):
        d = Dictionary()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        i = cls()
        for pair in p.pairs:
            i[pair.key] = json.loads(pair.value)
        return i

    def to_proto(self):
        d = Dictionary()
        pairs = []
        for k, v in self.items():
            pairs.append(Pair(key=str(k), value=json.dumps(v)))
        d.pairs.extend(pairs)
        return d

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def to_msgpack(self):
        return msgpack.packb(dict(self), use_bin_type=True)

    @classmethod
    def from_msgpack(cls, b):
        i = cls(dict(msgpack.unpackb(b, raw=False)))
        return i

    def to_dict(self):
        return dict(self)


class SerializableNamespaceDict(object):
    def __init__(self, namespace, data):
        if len(namespace) < 2:
            raise ValueError("Namespace should be at least 2 chars")

        if len(data) < 1:
            raise ValueError("Empty dict")

        self.namespace = namespace
        self.data = SerializableDict(data)

    @classmethod
    def from_proto_bytes(cls, b):
        d = MetaDataDict()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        d = {}
        for pair in p.pairs:
            d[pair.key] = json.loads(pair.value)
        return cls(p.namespace, d)

    def to_proto(self):
        d = MetaDataDict(namespace=self.namespace)
        pairs = []
        for k, v in self.data.items():
            pairs.append(Pair(key=str(k), value=json.dumps(v)))
        d.pairs.extend(pairs)
        return d

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def to_dict(self):
        return self.data.to_dict()


class ReaderActivityItem(object):
    def __init__(self, day_hour, reader_id, device_ids):
        self.day_hour = day_hour
        self.reader_id = reader_id
        self.device_ids = list(device_ids)

    def __repr__(self):
        return "<{}.{}: {}>".format(
            self.reader_id, self.day_hour, self.device_ids)

    @classmethod
    def from_proto_bytes(cls, b):
        d = ReaderActivity()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        return cls(p.day_hour, p.reader_id, p.device_ids)

    def to_proto(self):
        d = ReaderActivity(day_hour=self.day_hour, reader_id=self.reader_id,
                           device_ids=list(self.device_ids))
        return d

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def to_dict(self):
        return {"day_hour": self.day_hour_dt, "reader_id": self.reader_id, "device_ids": self.device_ids}

    @property
    def day_hour_dt(self):
        y = int(self.day_hour[0:4])
        m = int(self.day_hour[4:6])
        d = int(self.day_hour[6:8])
        h = int(self.day_hour[8:10])
        return pendulum.datetime(y, m, d, h)


class DeviceActivityItem(object):
    def __init__(self, day_hour, device_id, counter):
        self.day_hour = day_hour
        self.device_id = device_id
        self.counter = int(counter)

    def __repr__(self):
        return "<{}.{}: {}>".format(
            self.device_id, self.day_hour, self.counter)

    @classmethod
    def from_proto_bytes(cls, b):
        d = DeviceActivity()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        return cls(p.day_hour, p.device_id, p.counter)

    def to_proto(self):
        d = DeviceActivity(day_hour=self.day_hour, device_id=self.device_id,
                           counter=int(self.counter))
        return d

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def to_dict(self):
        return {"day_hour": self.day_hour_dt, "device_id": self.device_id, "counter": self.counter}

    @property
    def day_hour_dt(self):
        y = int(self.day_hour[0:4])
        m = int(self.day_hour[4:6])
        d = int(self.day_hour[6:8])
        h = int(self.day_hour[8:10])
        return pendulum.datetime(y, m, d, h)
