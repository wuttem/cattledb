#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals

from collections import defaultdict
from ..storage.models import TimeSeries, SeriesType


class CacheMiss(KeyError):
    pass


class BoundedTimeSeries(TimeSeries):
    def __init__(self, id, metric, cache_time=None, cache_points=None, max_interval=None):
        super(BoundedTimeSeries, self).__init__(key=id, metric=metric,
                                                series_type=SeriesType.FLOATSERIES)
        self.cache_time = cache_time
        self.cache_points = cache_points
        self.max_interval = max_interval

    def trim_to_bound(self):
        if self.cache_time is not None:
            min_ts = self.ts_max - self.cache_time
            self.trim(min_ts, self.ts_max)
        if self.cache_points is not None:
            self.trim_count_newest(self.cache_points)

    def is_expired(self, ts):
        if self.cache_time is not None:
            min_ts = self.ts_max - self.cache_time
            if ts < min_ts:
                return True
        if self.cache_points is not None:
           if self.get_index_below_ts(ts) is None:
               return True
        return False

    def insert_point_bounded(self, dt, value, overwrite=False):
        res = self.insert_point(dt, value, overwrite=overwrite)
        if res != 0:
            self.trim_to_bound()


class TimeSeriesCache(object):
    def __init__(self, default_cache_time=24*60*60, default_max_interval=None, default_max_points=None):
        self.default_cache_time = default_cache_time
        self.default_max_interval = default_max_interval
        self.default_max_points = default_max_points

        self.store = {}
        self.metric_parameters = defaultdict(dict)

    # Public Interface

    def set_cache_time_for_metric(self, metric, cache_time):
        self.metric_parameters[metric]["cache_time"] = cache_time

    def get_cache_time_for_metric(self, metric):
        return self.metric_parameters[metric].get("cache_time", self.default_cache_time)

    def set_max_interval_for_metric(self, metric, interval):
        self.metric_parameters[metric]["max_interval"] = interval

    def get_max_interval_for_metric(self, metric):
        return self.metric_parameters[metric].get("max_interval", self.default_max_interval)

    def set_max_points_for_metric(self, metric, points):
        self.metric_parameters[metric]["max_points"] = points

    def get_max_points_for_metric(self, metric):
        return self.metric_parameters[metric].get("max_points", self.default_max_points)

    def insert_point(self, id, metric, ts, value, ts_offset=0, overwrite=False):
        store = self._get_or_create_store_entry(id, metric)
        return store.insert_point((ts, ts_offset), value, overwrite=overwrite)

    def get_range(self, id, metric, from_ts, to_ts):
        store = self._get_or_create_store_entry(id, metric)
        if store.empty():
            raise CacheMiss("empty cache")
        # TODO: Raise on Store with Hole ?
        if store.is_expired(from_ts):
            raise CacheMiss("from_ts {} already expired".format(from_ts))
        return list(store.yield_range(from_ts, to_ts))

    # Internal Methods

    def _create_new_entry(self, id, metric):
        cache_time = self.get_cache_time_for_metric(metric)
        max_interval = self.get_max_interval_for_metric(metric)
        cache_points = self.get_max_points_for_metric(metric)
        return BoundedTimeSeries(id, metric, cache_time=cache_time, cache_points=cache_points)

    def _get_or_create_store_entry(self, id, metric):
        key = "{}.{}".format(id, metric)
        if key in self.store:
            return self.store[key]
        store = self._create_new_entry(id, metric)
        self.store[key] = store
        return store

    # def __getitem__(self, pos):
    #     id, metric = pos
    #     key = "{}.{}".format(x, y)
    #     if key in self.store:
    #         return self.store[key]
    #     raise CacheMiss("%s not found in TimeSeriesCache" % key)
