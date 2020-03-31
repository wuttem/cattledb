#!/usr/bin/python
# coding: utf-8

import unittest
import pendulum
import random
import logging
import binascii
import datetime

from cattledb.core.models import FastFloatTimeseries, FastDictTimeseries


class ModelTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_basic_timeseries(self):
        ts1 = FastFloatTimeseries("hello", "world")
        assert ts1.empty() == True

        ts1.insert_point((1100, 1800), 5.1)
        ts1.insert(
            [((1200, 1800), 5.2),
             ((1300, 1800), 5.3),
             ((1500, 1800), 5.5),
             ((1400, 1800), 5.4)])
        assert ts1.empty() == False
        assert len(ts1) == 5

        assert ts1.ts_min == 1100
        assert ts1.ts_max == 1500
        assert ts1.first.ts == 1100
        assert ts1.last.ts == 1500

        ts1.trim_count_newest(6)
        assert len(ts1) == 5
        ts1.trim_count_oldest(3)
        assert len(ts1) == 3
        ts1.trim_count_newest(1)
        assert len(ts1) == 1

    def test_ts_trim(self):
        events = [
            (pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, {"foo2": "bar2"}),
            (pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo4": "bar4"}),
            (pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"}),
            (pendulum.datetime(2015, 2, 5, 18, 0, tz='UTC').int_timestamp, {"foo3": "bar3"})
        ]
        ts1 = FastDictTimeseries("hello", "world")
        assert ts1.empty() == True

        ts1.insert(events)
        assert len(ts1) == 4
        t1 = pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp
        t2 = pendulum.datetime(2015, 2, 5, 17, 0, tz='UTC').int_timestamp
        ts1.trim(t1, t2)
        assert len(ts1) == 2

    def test_local_aggregation(self):
        ts1 = FastFloatTimeseries("ddd", "temp")
        ts2 = FastFloatTimeseries("fff", "temp")

        # daily, timeshift to winter
        start = pendulum.datetime(2018, 10, 25, 0, 0, tz='Europe/Vienna')
        cur = start
        for i in range(10):
            for j in range(24 * 6):
                ts1.insert_point(cur, float(i+1))
                cur = cur.add(minutes=10)
        end = cur
        self.assertEqual(start.add(days=10).subtract(hours=1), end)

        l = [list(x) for x in ts1.daily_local()]
        self.assertEqual(len(l), 10)
        self.assertEqual(len(l[0]), 144)
        self.assertEqual(l[0][0].dt, start)
        for i in range(144):
            self.assertEqual(l[0][i].value, 1.0)
        self.assertEqual(l[1][0].dt, start.add(days=1))
        for i in range(144):
            self.assertEqual(l[1][i].value, 2.0)

        l = list(ts1.aggregation("daily", "mean", raw=False, tz_mode="local"))
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0].dt, start)
        self.assertEqual(l[-1].dt, start.add(days=9))

        l = list(ts1.aggregation("daily", "mean", raw=True, tz_mode="local"))
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0].ts, start.int_timestamp)
        self.assertEqual(l[0].ts_offset, 7200)
        self.assertEqual(l[0].value, 1.0)
        self.assertEqual(l[-1].ts, end.start_of("day").int_timestamp)
        self.assertEqual(l[-1].ts_offset, 3600)
        self.assertEqual(l[-1].value, 10.0)

        # daily, timeshift to summer
        start = pendulum.datetime(2018, 3, 20, 0, 0, tz='Europe/Vienna')
        cur = start
        for i in range(10):
            for j in range(24 * 6):
                ts2.insert_point(cur, float(i+1))
                cur = cur.add(minutes=10)
        end = cur
        self.assertEqual(start.add(days=10).add(hours=1), end)

        l = list(ts2.aggregation("daily", "all", raw=False, tz_mode="local"))
        print(l)
        self.assertEqual(len(l), 11)
        self.assertEqual(l[0].dt, start)
        self.assertEqual(l[-1].dt, start.add(days=10))

        l = list(ts2.aggregation("daily", "all", raw=True, tz_mode="local"))
        self.assertEqual(len(l), 11)
        self.assertEqual(l[0].ts, start.int_timestamp)
        self.assertEqual(l[0].ts_offset, 3600)
        self.assertEqual(l[0].value.count, 144)
        self.assertEqual(l[0].value.mean, 1.0)
        self.assertEqual(l[0].value.stdev, 0.0)
        self.assertEqual(l[5].value.count, 144 - 6)
        self.assertEqual(l[-1].ts, end.start_of("day").int_timestamp)
        self.assertEqual(l[-1].ts_offset, 7200)
        self.assertEqual(l[-1].value.count, 6)
        self.assertEqual(l[-1].value.mean, 10.0)
        self.assertEqual(l[-1].value.stdev, 0.0)