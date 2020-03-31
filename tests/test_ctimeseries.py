#!/usr/bin/python
# coding: utf-8

import unittest
import random
import pendulum
import logging
import json

from cattledb.core._timeseries import FastFloatTSList


class CTimeSeriesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

    def test_base(self):
        t1 = FastFloatTSList("hellö", "world")

        end = pendulum.now("Europe/Vienna")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n).isoformat(), random.random()) for n in range(0, 200)]
        random.shuffle(data)

        for ts, val in data:
            t1.insert_iso(ts, val)
        t1_json = json.dumps(t1.serializable())

        j1 = json.loads(t1_json)

        assert len(j1) == 200
        assert len(t1.to_list()) == 200

        assert t1.at_index(0)[1] == start.offset
        assert t1.at_index(0)[0] == start.int_timestamp

        assert t1.iso_at_index(0)[0] == start.replace(microsecond=0).isoformat()
        assert t1.iso_at_index(len(t1)-1)[0] == end.replace(microsecond=0).isoformat()

    def test_timezone(self):
        t1 = FastFloatTSList("abc", "def")
        dt = pendulum.now("Europe/Vienna").replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[1] == dt.offset
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.iso_at_index(0)[0] == iso_str

    def test_canada(self):
        t1 = FastFloatTSList("abc", "def")
        dt = pendulum.datetime(2019, 2, 12, 8, 15, 32, tz='America/Toronto').replace(microsecond=0)
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == -5*3600
        assert t1.iso_at_index(0)[0] == "2019-02-12T08:15:32-05:00"

    def test_vienna(self):
        t1 = FastFloatTSList("abc", "def")
        dt = pendulum.datetime(2008, 3, 3, 12, 0, 0, tz='Europe/Vienna')
        iso_str = dt.isoformat()
        t1.insert_iso(iso_str, 0.1)
        assert t1.at_index(0)[0] == dt.int_timestamp
        assert t1.at_index(0)[1] == 3600
        assert t1.iso_at_index(0)[0] == "2008-03-03T12:00:00+01:00"

    def test_trim(self):
        t1 = FastFloatTSList("a", "b")

        end = pendulum.now("Europe/Vienna")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n), random.random()) for n in range(0, 200)]

        for dt, val in data:
            t1.insert_datetime(dt, val)

        assert t1._data.bisect_left(0) == 0
        assert t1._data.bisect_right(0) == 0

        assert len(t1) == 200
        t1.trim_index(100, 200)
        assert len(t1) == 100
        t1.trim_ts(start, end)
        assert len(t1) == 100
        t1.trim_ts(end.subtract(minutes=9), end)
        assert len(t1) == 10
        t1.trim_index(0, 0)
        assert len(t1) == 1
        t1.trim_index(1, 1)
        assert len(t1) == 0

    def test_trim_exact(self):
        t1 = FastFloatTSList("a", "b")

        t1.insert(100, 0, 2.2)
        t1.insert(200, 0, 2.2)
        t1.insert(300, 0, 2.2)
        t1.insert(400, 0, 2.2)

        assert len(t1) == 4
        t1.trim_ts(100, 400)
        assert len(t1) == 4
        t1.trim_ts(100, 399)
        assert len(t1) == 3
        t1.trim_ts(99, 399)
        assert len(t1) == 3
        t1.trim_ts(101, 399)
        assert len(t1) == 2
        t1.trim_ts(0, 399)
        assert len(t1) == 2
        t1.trim_ts(200, 300)
        assert len(t1) == 2
        t1.trim_ts(0, 1)
        assert len(t1) == 0

        # test right
        t2 = FastFloatTSList("a", "b")
        ts = pendulum.now("utc").int_timestamp
        t2.insert_datetime(ts, float(2.2))
        self.assertEqual(len(t2), 1)
        t2.trim_ts(ts, ts+1)
        self.assertEqual(len(t2), 1)
        t2.trim_ts(ts+1, ts+2)
        self.assertEqual(len(t2), 0)

        # test left
        t3 = FastFloatTSList("a", "b")
        ts = pendulum.now("utc").int_timestamp
        t3.insert_datetime(ts, float(2.2))
        self.assertEqual(len(t3), 1)
        t3.trim_ts(ts-1, ts)
        self.assertEqual(len(t3), 1)
        t3.trim_ts(ts-2, ts-1)
        self.assertEqual(len(t3), 0)

    def test_index(self):
        t = FastFloatTSList("a", "b")

        end = pendulum.now("America/Toronto")
        start = end.subtract(minutes=199)

        data = [(start.add(minutes=n), random.random()) for n in range(0, 200)]
        for dt, val in data:
            t.insert_datetime(dt, val)

        print(t.at_index(199))
        assert t.index_of_ts(end) == 199
        assert t.index_of_ts(end.subtract(minutes=3)) == 196

        with self.assertRaises(KeyError):
            t.index_of_ts(end.subtract(seconds=1))

        with self.assertRaises(KeyError):
            del t[-1]
        with self.assertRaises(KeyError):
            t.index_of_ts(end.add(seconds=1))

        last_ts = t.nearest_index_of_ts(end.add(seconds=1))
        assert last_ts == 199

        prev_ts = t.nearest_index_of_ts(end.subtract(seconds=40))
        assert prev_ts == 198
