#!/usr/bin/python
# coding: utf-8

import unittest
import pendulum
import random
import logging
import binascii
import datetime

from cattledb.storage.models import TimeSeries, SerializableDict, EventList
from cattledb.core.helper import (to_ts, daily_timestamps, monthly_timestamps,
                                  ts_daily_left, ts_daily_right,
                                  ts_weekly_left, ts_weekly_right,
                                  ts_monthly_left, ts_monthly_right,
                                  merge_lists_on_key)
from cattledb.grpcserver.cdb_pb2 import FloatTimeSeries, Dictionary


class ObjWithName(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value


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

    def test_aggregations(self):
        res = TimeSeries("ddd", "temp")
        ts = to_ts(datetime.datetime(2000, 1, 1, 0, 0))
        for _ in range(10):
            for j in range(144):
                res.insert_point(ts + j * 600, float(j % 6))
            ts += 144 * 600

        # All
        self.assertEqual(len(list(res.all())), 144 * 10)

        # Daily
        daily = list(res.daily())
        self.assertEqual(len(daily), 10)
        self.assertEqual(len(list(daily[0])), 144)

        # Daily Aggr
        g = res.aggregation("daily", "sum")
        for x in g:
            self.assertEqual(x[1], 360.0)

        g = res.aggregation("daily", "count")
        for x in g:
            self.assertEqual(x[1], 144)

        g = res.aggregation("daily", "mean")
        for x in g:
            self.assertEqual(x[1], 2.5)

        g = res.aggregation("daily", "min")
        for x in g:
            self.assertEqual(x[1], 0.0)

        g = res.aggregation("daily", "max")
        for x in g:
            self.assertEqual(x[1], 5.0)

        g = res.aggregation("daily", "amp")
        for x in g:
            self.assertEqual(x[1], 5.0)

        # Hourly
        daily = list(res.daily())
        self.assertEqual(len(daily), 10)
        self.assertEqual(len(list(daily[0])), 144)

        # Hourly Aggr
        g = res.aggregation("hourly", "sum")
        for x in g:
            self.assertEqual(x[1], 15.0)

        g = res.aggregation("hourly", "count")
        for x in g:
            self.assertEqual(x[1], 6)

        g = res.aggregation("hourly", "mean")
        for x in g:
            self.assertEqual(x[1], 2.5)

        g = res.aggregation("hourly", "min")
        for x in g:
            self.assertEqual(x[1], 0.0)

        g = res.aggregation("hourly", "max")
        for x in g:
            self.assertEqual(x[1], 5.0)

        g = res.aggregation("hourly", "amp")
        for x in g:
            self.assertEqual(x[1], 5.0)

    def test_dictitem(self):
        i1 = EventList("test", "ph")
        self.assertTrue(i1.empty())
        i1.insert_point(1, dict(hey=1.0, ho=2.0))
        self.assertFalse(i1.empty())

        i2 = TimeSeries("test1", "ph", [(1, 1.0)])
        self.assertEqual(i2[0].value, 1.0)
        i2.insert_point(1, 2.0)
        self.assertEqual(i2[0].value, 2.0)

        i3 = TimeSeries("test2", "ph", [(1, 1.0)])
        self.assertNotEqual(i2, i3)

        i4 = TimeSeries("test1", "ph", [(1, 1.0)])
        self.assertEqual(i2, i4)

        self.assertEqual(i2.to_hash(), i4.to_hash())

    def test_intdata(self):
        i = TimeSeries("int", "ph")
        for j in range(10):
            i.insert_point(j, int(j * 2.1))
        self.assertEqual(len(i), 10)
        self.assertEqual(i[3].ts, 3)
        self.assertEqual(i[3].value, 6)

    def test_rawitem(self):
        d = []
        for i in range(100):
            d.append((i, i * 2.5))
        self.assertEqual(len(d), 100)

        d1 = list(d[:50])
        d2 = list(d[50:])
        random.shuffle(d1)
        random.shuffle(d2)

        i = TimeSeries("ph_sensor", "ph")
        for t, v in d1:
            i.insert_point(t, v)
        i.insert(d2)

        l = list([x for x in i.all()])
        self.assertEqual(len(l), 100)
        logging.warning(l)
        for i in range(100):
            self.assertEqual(l[i][0], i)
            self.assertEqual(l[i][1], i * 2.5)

    def test_daily_timestamps(self):
        x = list(daily_timestamps(0, 0))
        self.assertEqual(x, [0])
        x = list(daily_timestamps(0, 24*60*60-1))
        self.assertEqual(x, [0])
        x = list(daily_timestamps(0, 24*60*60))
        self.assertEqual(x, [0, 86400])
        x = list(daily_timestamps(0, 24*60*60+23))
        self.assertEqual(x, [0, 86400])
        x = list(daily_timestamps(0, 2*24*60*60-1))
        self.assertEqual(x, [0, 86400])
        x = list(daily_timestamps(0, 2*24*60*60))
        self.assertEqual(x, [0, 86400, 2*24*60*60])

    def test_monthly_timestamps(self):
        x = list(monthly_timestamps(0, 0))
        self.assertEqual(x, [0])
        x = list(monthly_timestamps(0, 31*24*60*60-1))
        self.assertEqual(x, [0])
        x = list(monthly_timestamps(0, 31*24*60*60))
        self.assertEqual(x, [0, 31*24*60*60])
        x = list(monthly_timestamps(0, (31+27)*24*60*60))
        self.assertEqual(x, [0, 31*24*60*60])
        x = list(monthly_timestamps(0, (31+28)*24*60*60-1))
        self.assertEqual(x, [0, 31*24*60*60])
        x = list(monthly_timestamps(0, (31+28)*24*60*60))
        self.assertEqual(x, [0, 31*24*60*60, (31+28)*24*60*60])
        x = list(monthly_timestamps(0, (31+28+30)*24*60*60))
        self.assertEqual(x, [0, 31*24*60*60, (31+28)*24*60*60])
        x = list(monthly_timestamps(0, (31+28+31)*24*60*60))
        self.assertEqual(x, [0, 31*24*60*60, (31+28)*24*60*60, (31+28+31)*24*60*60])

    def test_left_right(self):
        test_dts = [pendulum.datetime(2000, 1, 15, 12, 1),
                    pendulum.datetime(2000, 3, 21, 14, 45),
                    pendulum.datetime(2000, 2, 29, 12, 1),
                    pendulum.datetime(2001, 2, 28, 12, 1),
                    pendulum.datetime(2014, 11, 21, 14, 45),
                    pendulum.datetime(2000, 10, 1, 12, 1),
                    pendulum.datetime(2000, 3, 1, 14, 45),
                    pendulum.datetime(2000, 8, 30, 12, 1),
                    pendulum.datetime(2000, 7, 31, 14, 45)]
        for dt in test_dts:
            self.assertEqual(ts_daily_left(dt.int_timestamp),
                            dt.start_of("day").int_timestamp)
            self.assertEqual(ts_daily_right(dt.int_timestamp),
                            dt.end_of("day").int_timestamp)
            self.assertEqual(ts_weekly_left(dt.int_timestamp),
                            dt.start_of("week").int_timestamp)
            self.assertEqual(ts_weekly_right(dt.int_timestamp),
                            dt.end_of("week").int_timestamp)
            self.assertEqual(ts_monthly_left(dt.int_timestamp),
                            dt.start_of("month").int_timestamp)
            self.assertEqual(ts_monthly_right(dt.int_timestamp),
                            dt.end_of("month").int_timestamp)

    def test_proto(self):
        res = TimeSeries("ddd", "ph")
        ts = to_ts(datetime.datetime(2000, 1, 1, 0, 0))
        for j in range(500):
            res.insert_point(j * 600, float(j % 6))
        p = FloatTimeSeries()
        p.ParseFromString(res.to_proto_bytes())
        self.assertEqual(len(p.timestamps), 500)
        self.assertEqual(len(p.timestamp_offsets), 500)
        self.assertEqual(len(p.values), 500)
        self.assertEqual(p.key, "ddd")
        self.assertEqual(p.metric, "ph")

        ts2 = TimeSeries.from_proto(p)
        self.assertEqual(len(ts2), 500)

        ts3 = TimeSeries.from_proto_bytes(ts2.to_proto_bytes())
        self.assertEqual(len(ts3), 500)

    def test_dict(self):
        d1 = SerializableDict({"hello": "wörld", "föö":"bär"})
        d2 = SerializableDict({"1": 1, 2:3.4})

        self.assertEqual(d1["hello"], "wörld")
        self.assertEqual(d2["1"], 1)
        self.assertEqual(d2[2], 3.4)

        d3 = SerializableDict.from_proto_bytes(d1.to_proto_bytes())
        self.assertEqual(d3["föö"], "bär")

        d4 = SerializableDict.from_msgpack(d1.to_msgpack())
        self.assertEqual(d3["föö"], "bär")

        self.assertTrue(isinstance(d4, dict))

    def test_local_aggregation(self):
        ts1 = TimeSeries("ddd", "temp")
        ts2 = TimeSeries("fff", "temp")

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

    def test_merge_lists(self):
        m1 = ObjWithName("ph", "ph1")
        m2 = ObjWithName("temp", "temp1")
        m3 = ObjWithName("act", "act1")
        m4 = ObjWithName("act", "act2")
        m5 = ObjWithName("hum", "hum1")
        a = [m1, m2, m3]
        b = [m4, m5]
        m = merge_lists_on_key(a, b, key=lambda x: x.name)
        self.assertEqual(len(m), 4)
        self.assertEqual([x.value for x in m], ["ph1", "temp1", "act2", "hum1"])

        m2 = merge_lists_on_key(a, b, key=lambda x: x.value)
        self.assertEqual(len(m2), 5)
        self.assertEqual([x.value for x in m2], ["ph1", "temp1", "act1", "act2", "hum1"])
