#!/usr/bin/python
# coding: utf8

from enum import Enum
from collections import MutableSequence
from collections import namedtuple

import bisect
import logging
import array
import hashlib

from .helper import ts_daily_left, ts_daily_right
from .helper import ts_hourly_left, ts_hourly_right
from .helper import ts_weekly_left, ts_weekly_right
from .helper import ts_monthly_left, ts_monthly_right


Aggregation = namedtuple('Aggregation', ['min', 'max', 'sum', 'count'])


class TimeSeries(object):
    def __init__(self, key, values=None):
        self._timestamps = array.array("I")
        self._values = list()
        if values is not None:
            self.insert(values)
        self.key = key.lower()
        assert len(self.key) >= 2

    @classmethod
    def new(cls, key, values=None):
        """Factory Method to create Items.
        """
        return cls(key, values)

    def __len__(self):
        return len(self._timestamps)

    def __bool__(self):  # Python 3
        if len(self) < 1:
            return False
        
        assert len(self._timestamps) == len(self._values)
        self.checkSorted()
        return True

    def checkSorted(self):
        it = iter(self._timestamps)
        it.__next__()
        assert all(b >= a for a, b in zip(self._timestamps, it))

    def to_hash(self):
        s = "{}.{}.{}.{}".format(self.key, len(self), 
                                 self.ts_min, self.ts_max)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, TimeSeries):
            return False
        # Is Hashing a Performance Problem ?
        h1 = self.to_hash()
        h2 = other.to_hash()
        return h1 == h2

    def __ne__(self, other):
        return not self == other  # NOT return not self.__eq__(other)

    def __repr__(self):
        l = len(self._timestamps)
        if l > 0:
            m = self._timestamps[0]
        else:
            m = -1
        return "<{} series({}), min_ts: {}>".format(
            self.key, l, m)

    @property
    def ts_max(self):
        if len(self._timestamps) > 0:
            return self._timestamps[-1]
        return -1

    @property
    def ts_min(self):
        if len(self._timestamps) > 0:
            return self._timestamps[0]
        return -1

    @property
    def count(self):
        return len(self._timestamps)

    def _at(self, i):
        return (self._timestamps[i], self._values[i])

    def __getitem__(self, key):
        return self._at(key)

    def to_list(self):
        out = list()
        for i in range(len(self._timestamps)):
            out.append(self._at(i))
        return out

    def insert_point(self, timestamp, value, overwrite=False):
        timestamp = int(timestamp)
        idx = bisect.bisect_left(self._timestamps, timestamp)
        # Append
        if idx == len(self._timestamps):
            self._timestamps.append(timestamp)
            self._values.append(value)
            return 1
        # Already Existing
        if self._timestamps[idx] == timestamp:
            # Replace
            logging.debug("duplicate insert")
            if overwrite:
                self._values[idx] = value
                return 1
            return 0
        # Insert
        self._timestamps.insert(idx, timestamp)
        self._values.insert(idx, value)
        return 1

    def insert(self, series):
        counter = 0
        for timestamp, value in series:
            counter += self.insert_point(timestamp, value)
        self.checkSorted() # may be removed
        return counter

    def _trim(self, ts_min, ts_max):
        low = bisect.bisect_left(self._timestamps, ts_min)
        high = bisect.bisect_right(self._timestamps, ts_max)
        self._timestamps = self._timestamps[low:high]
        self._values = self._values[low:high]

    def all(self):
        """Return an iterator to get all ts value pairs.
        """
        return zip(self._timestamps, self._values)

    def daily(self):
        """Generator to access daily data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._timestamps):
            j = 0
            lower_bound = ts_daily_left(self._timestamps[i])
            upper_bound = ts_daily_right(self._timestamps[i])
            while (i + j < len(self._timestamps) and
                   lower_bound <= self._timestamps[i + j] <= upper_bound):
                j += 1
            yield ((self._timestamps[x], self._values[x])
                   for x in range(i, i + j))
            i += j

    def daily_buckets(self):
        i = 0
        while i < len(self._timestamps):
            j = 0
            lower_bound = ts_daily_left(self._timestamps[i])
            upper_bound = ts_daily_right(self._timestamps[i])
            while (i + j < len(self._timestamps) and
                   lower_bound <= self._timestamps[i + j] <= upper_bound):
                j += 1
            yield (lower_bound, [(self._timestamps[x], self._values[x])
                   for x in range(i, i + j)])
            i += j

    def hourly(self):
        """Generator to access hourly data.
        This will return an inner generator.
        """
        i = 0
        while i < len(self._timestamps):
            j = 0
            lower_bound = ts_hourly_left(self._timestamps[i])
            upper_bound = ts_hourly_right(self._timestamps[i])
            while (i + j < len(self._timestamps) and
                   lower_bound <= self._timestamps[i + j] <= upper_bound):
                j += 1
            yield ((self._timestamps[x], self._values[x])
                   for x in range(i, i + j))
            i += j

    def aggregation(self, group="hourly", function="mean"):
        """Aggregation Generator.
        """
        if group == "hourly":
            it = self.hourly
            left = ts_hourly_left
        elif group == "daily":
            it = self.daily
            left = ts_daily_left
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
                return sum(x) / len(x)
            func = mean
        else:
            raise ValueError("Invalid aggregation group")

        for g in it():
            t = list(g)
            ts = left(t[0][0])
            value = func([x[1] for x in t])
            yield (ts, value)
