#!/usr/bin/python
# coding: utf8

from __future__ import unicode_literals

import unittest
import random
import logging


from cattledb.cache import TimeSeriesCache, CacheMiss


class CacheTest(unittest.TestCase):
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

    def test_cache(self):
        cache = TimeSeriesCache(default_cache_time=10)
        for i in range(101):
            cache.insert_point("sensor1", "temp", i, i*2)

        r = cache.get_range("sensor1", "temp", 93, 96)
        ts_list = [x.ts for x in r]
        value_list = [x.value for x in r]
        self.assertEqual(ts_list, [93, 94, 95, 96])
        self.assertEqual(value_list, [93*2, 94*2, 95*2, 96*2])

        with self.assertRaises(CacheMiss):
            r = cache.get_range("sensor1", "temp", 89, 91)

        r = cache.get_range("sensor1", "temp", 90, 191)
        ts_list = [x.ts for x in r]
        value_list = [x.value for x in r]
        self.assertEqual(ts_list, list(range(90, 101)))

        r = cache.get_range("sensor1", "temp", 100, 100)
        ts_list = [x.ts for x in r]
        value_list = [x.value for x in r]
        self.assertEqual(ts_list, [100])
        self.assertEqual(value_list, [200])
