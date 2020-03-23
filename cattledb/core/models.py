#!/usr/bin/python
# coding: utf-8

import abc
import hashlib
import pendulum
import struct
import msgpack
import json

from enum import Enum
from collections import namedtuple, defaultdict
from statistics import stdev, mean, median

from .helper import ts_daily_left, ts_daily_right
from .helper import ts_monthly_left, ts_monthly_right
from .helper import ts_hourly_left, ts_hourly_right
from .helper import list_mean
from ._timeseries import FloatTSList, PyTSList


from ..grpcserver.cdb_pb2 import FloatTimeSeries, Dictionary, DictTimeSeries, Pair, MetaDataDict, ReaderActivity, DeviceActivity, EventSeries


Point = namedtuple('Point', ['ts', 'value', 'dt'])
RawPoint = namedtuple('RawPoint', ['ts', 'value', 'ts_offset'])
MetaDataItem = namedtuple('MetaDataItem', ["object_name", "object_id", "key", "data"])
_AggregationValue = namedtuple("AggregationValue", ["count", "sum", "min", "max", "mean", "stdev", "median"])
RowUpsert = namedtuple('RowUpsert', ['row_key', 'cells'])


class MetricType(Enum):
    FLOATSERIES = 1
    DICTSERIES = 2


class EventSeriesType(Enum):
    DAILY = 1
    MONTHLY = 2


class MetricDefinition(object):
    def __init__(self, name, id, type, delete_possible, _deprecated1=None, _deprecated2=None):
        self.name = name
        self.id = id
        self.type = type
        self.delete_possible = delete_possible

    @classmethod
    def from_dict(cls, d):
        name = d["name"]
        id = d["id"]
        t = MetricType(d["type"])
        delete_possible = d["delete_possible"]
        return cls(name, id, t, delete_possible)

    def to_dict(self):
        return {
            'name': self.name,
            'id': self.id,
            'type': self.type.value,
            'delete_possible': self.delete_possible
        }

    def __repr__(self):
        return "Metric: {} (id={}, type={})".format(self.name, self.id, self.type)


class EventDefinition(object):
    def __init__(self, name, type, _deprecated1=None):
        self.name = name
        self.type = type

    @classmethod
    def from_dict(cls, d):
        name = d["name"]
        t = EventSeriesType(d["type"])
        return cls(name, t)

    def to_dict(self):
        return {
            'name': self.name,
            'type': self.type.value
        }

    def __repr__(self):
        return "Event: {} (type={})".format(self.name, self.type)


class AggregationValue(_AggregationValue):
    def to_dict(self):
        return dict(self._asdict())


def full_aggregation(x):
    if len(x) <= 1:
        return AggregationValue(len(x), 0, 0, 0, 0, 0, 0)

    return AggregationValue(
        count=len(x), sum=sum(x), min=min(x),
        max=max(x), mean=mean(x), stdev=stdev(x),
        median=median(x))


class BaseTimeseries(object, metaclass=abc.ABCMeta):
    __container__ = None

    def __init__(self, key, metric, values=None):
        self._data = self.__container__(key.lower(), metric.lower())
        if values is not None:
            self.insert(values)
        self.key = key.lower()
        self.metric = metric.lower()

    def __len__(self):
        return len(self._data)

    def set_metric(self, m):
        self.metric = m.lower()

    def to_hash(self):
        s = "{}.{}.{}.{}.{}".format(self.key, self.metric, len(self),
                                    self.ts_min, self.ts_max)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        h1 = self.to_hash()
        h2 = other.to_hash()
        return h1 == h2

    def __repr__(self):
        l = len(self._data)
        if l > 0:
            m = self._data.at_index(0)[0]
        else:
            m = -1
        return "<{}.{} series({}), min_ts: {}>".format(
            self.key, self.metric, l, m)

    @property
    def ts_max(self):
        if self.empty():
            return None
        return self._raw_at(len(self)-1)[0]

    @property
    def ts_min(self):
        if self.empty():
            return None
        return self._raw_at(0)[0]

    def empty(self):
        return bool(len(self) < 1)

    @property
    def first(self):
        return None if self.empty() else self._rawpoint_at(0)

    @property
    def last(self):
        return None if self.empty() else self._rawpoint_at(len(self)-1)

    def _at(self, index, raw=False):
        if raw:
            return self._rawpoint_at(index)
        return self._point_at(index)

    def _rawpoint_at(self, index):
        if index < 0 or index >= len(self):
            raise IndexError
        ts, ts_offset, value = self._data.at_index(index)
        return RawPoint(ts=ts, value=value, ts_offset=ts_offset)

    def _raw_at(self, index):
        if index < 0 or index >= len(self):
            raise IndexError
        return self._data.at_index(index)

    def _point_at(self, index):
        if index < 0 or index >= len(self):
            raise IndexError
        ts, ts_offset, value = self._data.at_index(index)
        dt = pendulum.from_timestamp(ts, ts_offset/3600.0)
        return Point(ts=ts, value=value, dt=dt)

    def _serializable_at(self, index):
        if index < 0 or index >= len(self):
            raise IndexError
        item = self._data.iso_at(index)
        return (item[0], item[1])

    def __getitem__(self, key):
        return self._point_at(key)

    def insert(self, series):
        counter = 0
        for timestamp, value in series:
            counter += self.insert_point(timestamp, value)
        return counter

    def trim(self, ts_min, ts_max):
        return self._data.trim_ts(ts_min, ts_max)

    def trim_count_newest(self, count):
        if count >= len(self):
            return self
        max_idx = len(self)-1
        min_idx = max_idx-(count-1)
        return self._data.trim_index(min_idx, max_idx)

    def trim_count_oldest(self, count):
        if count >= len(self):
            return self
        max_idx = count-1
        min_idx = 0
        return self._data.trim_index(min_idx, max_idx)

    def all(self, raw=False):
        """Return an iterator to get all ts value pairs.
        """
        i = 0
        while i < len(self):
            yield self._at(i, raw=raw)
            i += 1

    def yield_range(self, ts_min, ts_max, raw=False):
        """Return an iterator to get all ts value pairs in range.
        """
        low = self._data.bisect_left(ts_min)
        high = self._data.bisect_right(ts_max)

        i = low
        while i < high:
            yield self._at(i, raw=raw)
            i += 1

    def daily(self, raw=False):
        """Generator to access daily data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_daily_left(self._data.at_index(i)[0])
            upper_bound = ts_daily_right(self._data.at_index(i)[0])
            while (i + j < len(self) and
                   lower_bound <= self._data.at_index(i + j)[0] <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def monthly_storage_buckets(self):
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_monthly_left(self._data.at_index(i)[0])
            upper_bound = ts_monthly_right(self._data.at_index(i)[0])
            while (i + j < len(self) and
                   lower_bound <= self._data.at_index(i + j)[0] <= upper_bound):
                j += 1
            yield (lower_bound, [self._storage_item_at(x) for x in range(i, i + j)])
            i += j

    def daily_storage_buckets(self):
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_daily_left(self._data.at_index(i)[0])
            upper_bound = ts_daily_right(self._data.at_index(i)[0])
            while (i + j < len(self) and
                   lower_bound <= self._data.at_index(i + j)[0] <= upper_bound):
                j += 1
            yield (lower_bound, [self._storage_item_at(x) for x in range(i, i + j)])
            i += j

    def get_serializable_iterator(self, timestamp_format, aggregation_span=None, aggregation_type=None):
        """
        Possible formats: utc, local, iso, tuple, dt
        Aggregation Spans: hourly, daily, 10min
        Aggregation Types: mean, count, sum, min, max, all
        """
        data = []
        if timestamp_format == "utc":
            timestamp_func = lambda x: x.ts
        elif timestamp_format == "local":
            timestamp_func = lambda x: x.ts + x.ts_offset
        elif timestamp_format == "iso":
            timestamp_func = lambda x: pendulum.from_timestamp(x.ts, x.ts_offset/3600.0).isoformat()
        elif timestamp_format == "tuple":
            timestamp_func = lambda x: (x.ts, x.ts_offset)
        elif timestamp_format == "dt":
            timestamp_func = lambda x: pendulum.from_timestamp(x.ts, x.ts_offset/3600.0)
        else:
            raise ValueError("invalid timestamp format")
        if aggregation_span and aggregation_type:
            for p in self.aggregation(aggregation_span, aggregation_type, raw=True):
                yield (timestamp_func(p), p.value)
        else:
            for p in self.all(raw=True):
                yield (timestamp_func(p), p.value)

    # def to_serializable(self):
    #     return self._data.serializable()

    def aligned_10minute(self, raw=False):
        """Generator to data aligned to 10min period.
        This will return an inner generator.
        """
        i = 0
        while i < len(self):
            j = 0
            lower_bound = self._data.at_index(i)[0] - (self._data.at_index(i)[0] % (10*60))
            upper_bound = lower_bound + 10*60 - 1
            while (i + j < len(self) and
                   lower_bound <= self._data.at_index(i + j)[0] <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def hourly(self, raw=False):
        """Generator to access hourly data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_hourly_left(self._data.at_index(i)[0])
            upper_bound = ts_hourly_right(self._data.at_index(i)[0])
            while (i + j < len(self) and
                   lower_bound <= self._data.at_index(i + j)[0] <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def hourly_local(self, raw=False):
        """Generator to access hourly data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_hourly_left(self._data.at_index(i)[0] + self._data.at_index(i)[1])
            upper_bound = ts_hourly_right(self._data.at_index(i)[0] + self._data.at_index(i)[1])
            while (i + j < len(self) and
                   (self._data.at_index(i + j)[0] + self._data.at_index(i + j)[1]) <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def daily_local(self, raw=False):
        """Generator to access daily data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self):
            j = 0
            lower_bound = ts_daily_left(self._data.at_index(i)[0] + self._data.at_index(i)[1])
            upper_bound = ts_daily_right(self._data.at_index(i)[0] + self._data.at_index(i)[1])
            while (i + j < len(self) and
                   (self._data.at_index(i + j)[0] + self._data.at_index(i + j)[1]) <= upper_bound):
                j += 1
            yield (self._at(x, raw=raw) for x in range(i, i + j))
            i += j

    def aggregation(self, group="hourly", function="mean", raw=False,
                    tz_mode="utc"):
        """Aggregation Generator.
        """
        assert tz_mode in ["utc", "local"]

        if group == "hourly":
            if tz_mode == "local":
                it = self.hourly_local
            else:
                it = self.hourly
            left = ts_hourly_left
        elif group == "daily":
            if tz_mode == "local":
                it = self.daily_local
            else:
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
            func = list_mean
        elif function == "all":
            func = full_aggregation
        else:
            raise ValueError("Invalid aggregation group")

        if raw:
            for g in it(raw=True):
                t = list(g)
                if tz_mode == "local":
                    ts = left(t[0].ts + t[0].ts_offset) - t[0].ts_offset
                else:
                    ts = left(t[0].ts)
                offset = t[0].ts_offset
                value = func([x.value for x in t])
                yield RawPoint(ts, value, offset)
        else:
            for g in it(raw=True):
                t = list(g)
                if tz_mode == "local":
                    ts = left(t[0].ts + t[0].ts_offset) - t[0].ts_offset
                else:
                    ts = left(t[0].ts)
                offset = t[0].ts_offset
                dt = pendulum.from_timestamp(ts, offset/3600.0)
                value = func([x.value for x in t])
                yield Point(ts, value, dt)

    # TODO This should be done faster (maybe cdb_ext in c++)
    def _to_lists(self):
        timestamps = []
        values = []
        timestamp_offsets = []
        for ts, ts_offset, value in self._data:
            timestamps.append(ts)
            values.append(value)
            timestamp_offsets.append(ts_offset)
        return timestamps, values, timestamp_offsets

    @abc.abstractmethod
    def _storage_item_at(self, index):
        pass

    @abc.abstractmethod
    def insert_storage_item(self, index):
        pass

    @abc.abstractmethod
    def insert_point(self, dt, value):
        pass


class FastFloatTimeseries(BaseTimeseries):
    __container__ = FloatTSList

    def insert_point(self, dt, value):
        return self._data.insert_datetime(dt, float(value))

    def _storage_item_at(self, index):
        assert 0 <= index < len(self)
        item = self._data.at_index(index)
        by = struct.pack("B", 1) + struct.pack("i", item[1]) + struct.pack("f", item[2])
        return (item[0], by)

    def insert_storage_item(self, timestamp, by):
        f = int(struct.unpack("B", by[0:1])[0])
        assert f == 1
        offset = int(struct.unpack("i", by[1:5])[0])
        value = float(struct.unpack("f", by[5:9])[0])
        return self._data.insert(ts=timestamp, ts_offset=offset, value=value)

    @classmethod
    def from_proto_bytes(cls, b):
        f = FloatTimeSeries()
        f.ParseFromString(b)
        return cls.from_proto(f)

    @classmethod
    def from_proto(cls, p):
        i = cls(p.key, p.metric)
        for ts, ts_offset, value in zip(p.timestamps, p.timestamp_offsets, p.values):
            i._data.insert(ts=ts, ts_offset=ts_offset, value=float(value))
        return i

    def to_proto(self):
        fts = FloatTimeSeries()
        for ts, ts_offset, value in self._data:
            fts.values.append(value)
            fts.timestamps.append(ts)
            fts.timestamp_offsets.append(ts_offset)
        fts.metric = self.metric
        fts.key = self.key
        return fts

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()


TimeSeries = FastFloatTimeseries


class FastDictTimeseries(BaseTimeseries):
    __container__ = PyTSList

    def __init__(self, *args, **kwargs):
        super(FastDictTimeseries, self).__init__(*args, **kwargs)
        self.columns = None

    def insert_point(self, dt, value):
        return self._data.insert_datetime(dt, dict(value))

    def _storage_item_at(self, index):
        assert 0 <= index < len(self)
        item = self._data.at_index(index)
        by = struct.pack("B", 2) + struct.pack("i", item[1]) + msgpack.packb(item[2], use_bin_type=True)
        return (item[0], by)

    def insert_storage_item(self, timestamp, by):
        f = int(struct.unpack("B", by[0:1])[0])
        assert f == 2
        offset = int(struct.unpack("i", by[1:5])[0])
        value = msgpack.unpackb(by[5:], raw=False)
        return self._data.insert(ts=timestamp, ts_offset=offset, value=value)

    @classmethod
    def from_proto_bytes(cls, b):
        d = DictTimeSeries()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        i = cls(p.key, p.metric)
        parsed_values = [SerializableDict.from_proto(x) for x in p.values]
        for ts, ts_offset, value in zip(p.timestamps, p.timestamp_offsets, parsed_values):
            i._data.insert(ts=ts, ts_offset=ts_offset, value=dict(value))
        return i

    def to_proto(self):
        dts = DictTimeSeries()
        p_values = []
        for ts, ts_offset, value in self._data:
            p_values.append(SerializableDict(value).to_proto())
            dts.timestamps.append(ts)
            dts.timestamp_offsets.append(ts_offset)
        dts.values.extend(p_values)
        dts.metric = self.metric
        dts.key = self.key
        return dts

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()

    def set_columns(self, column_list):
        self.columns = column_list

    @classmethod
    def from_float_timeseries(cls, *float_timeseries, key=None, ts_offset=0):
        assert len(float_timeseries) > 0
        keys = []
        metrics = []
        merged = defaultdict(dict)
        
        # check keys and metric
        for f in float_timeseries:
            keys.append(f.key)
            metrics.append(f.metric)

        for f in float_timeseries:
            for x in f.all(raw=True):
                m = f.metric
                merged[x.ts][m] = x.value

        key = key or keys[0]

        new_ts = cls(key=key, metric="multi")
        new_ts.set_columns(metrics)
        for k, v in merged.items():
            new_ts._data.insert(ts=k, ts_offset=ts_offset, value=v)

        return new_ts

    def yield_rows(self, timestamp_format="tuple", aggregation_span=None, aggregation_type=None):
        it = self.get_serializable_iterator(timestamp_format, aggregation_span=aggregation_span,
                                            aggregation_type=aggregation_type)
        if self.columns:
            for ts, value in it:
                if not value:
                    row = (ts, ) + tuple(None for _ in self.columns)
                else:
                    row = (ts, ) + tuple(value.get(c, None) for c in self.columns)
                yield(row)
        else:
            yield from it

    def to_csv(self, fp):
        import csv
        w = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        if self.columns:
            w.writerow(["ts"] + [x for x in self.columns])
        for row in self.yield_rows("utc"):
            w.writerow(row)

    def to_pandas(self):
        raise NotImplementedError

class EventList(FastDictTimeseries):
    @property
    def name(self):
        return self.metric

    @classmethod
    def from_proto_bytes(cls, b):
        d = EventSeries()
        d.ParseFromString(b)
        return cls.from_proto(d)

    @classmethod
    def from_proto(cls, p):
        i = cls(p.key, p.name)
        parsed_values = [SerializableDict.from_proto(x) for x in p.values]
        for ts, ts_offset, value in zip(p.timestamps, p.timestamp_offsets, parsed_values):
            i._data.insert(ts=ts, ts_offset=ts_offset, value=dict(value))
        return i

    def to_proto(self):
        dts = EventSeries()
        p_values = []
        for ts, ts_offset, value in self._data:
            p_values.append(SerializableDict(value).to_proto())
            dts.timestamps.append(ts)
            dts.timestamp_offsets.append(ts_offset)
        dts.values.extend(p_values)
        dts.name = self.name
        dts.key = self.key
        return dts

    def to_proto_bytes(self):
        p = self.to_proto()
        return p.SerializeToString()


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
