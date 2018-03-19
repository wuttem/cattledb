#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import binascii
import datetime

from cattledb.storage.models import TimeSeries, Aggregation
from cattledb.storage.helper import to_ts


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
        res = TimeSeries("ddd")
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

    def test_item(self):
        i1 = TimeSeries("test")
        self.assertFalse(i1)
        i1.insert_point(1, (1.0, 2.0, 3.0))
        self.assertTrue(i1)

        i2 = TimeSeries("test1", [(1, 1.0)])
        self.assertEqual(i2[0], (1, 1.0))
        i2.insert_point(1, 2.0)
        self.assertEqual(i2[0], (1, 1.0))
        i2.insert_point(1, 2.0, overwrite=True)
        self.assertEqual(i2[0], (1, 2.0))

        i3 = TimeSeries("test2", [(1, 1.0)])
        self.assertNotEqual(i2, i3)

        i4 = TimeSeries("test1", [(1, 1.0)])
        self.assertEqual(i2, i4)

        self.assertEqual(i2.to_hash(), i4.to_hash())

    def test_intdata(self):
        i = TimeSeries("int")
        for j in range(10):
            i.insert_point(j, int(j * 2.1))
        self.assertEqual(len(i), 10)
        self.assertEqual(i[3], (3, 6))

    def test_rawitem(self):
        d = []
        for i in range(100):
            d.append((i, i * 2.5))
        self.assertEqual(len(d), 100)

        d1 = list(d[:50])
        d2 = list(d[50:])
        random.shuffle(d1)
        random.shuffle(d2)

        i = TimeSeries("ph1")
        for t, v in d1:
            i.insert_point(t, v)
        i.insert(d2)

        l = i.to_list()
        self.assertEqual(len(l), 100)
        logging.warning(l)
        for i in range(100):
            self.assertEqual(l[i][0], i)
            self.assertEqual(l[i][1], i * 2.5)
