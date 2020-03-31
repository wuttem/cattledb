#!/usr/bin/python
# coding: utf-8

import unittest
import random
import logging
import pendulum
import os
import datetime
import mock
import time

from cattledb.storage.connection import Connection
from cattledb.storage.models import TimeSeries, FastDictTimeseries
from .helper import get_unit_test_config, get_test_metrics


class TimeSeriesStorageTest(unittest.TestCase):
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

    def test_simple(self):
        db = Connection.from_config(get_unit_test_config())
        db.database_init(silent=True)

        db.add_metric_definitions(get_test_metrics())
        db.store_metric_definitions()
        db.load_metric_definitions()

        db.timeseries._create_metric("ph", silent=True)
        db.timeseries._create_metric("act", silent=True)
        db.timeseries._create_metric("temp", silent=True)

        start = 1584521241
        t = int(start - 50 * 24 * 60 * 60)

        r = db.timeseries.delete_timeseries("sensor1", ["ph", "act", "temp"], t, t + 500*600 + 24 * 60 * 60)

        d1 = [(t + i * 600, 6.5) for i in range(502)]
        d2 = [(t + i * 600 + 24 * 60 * 60, 25.5) for i in range(502)]
        d3 = [(t + i * 600, 10.5) for i in range(502)]

        data = [{"key": "sensor1",
                 "metric": "ph",
                 "data": d1},
                {"key": "sensor1",
                 "metric": "temp",
                 "data": d2}]
        db.timeseries.insert_bulk(data)
        db.timeseries.insert("sensor1", "act", d3)
        db.timeseries.insert("sensor2", "ph", d3)

        r = db.timeseries.get_single_timeseries("sensor1", "act", t, t + 500*600-1)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 500)
        self.assertLessEqual(len(d), 5)
        for ts, v, dt in d:
            self.assertAlmostEqual(v, 10.5, 4)

        r = db.timeseries.get_single_timeseries("sensor1", "ph", t, t + 500*600-1)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 500)
        self.assertLessEqual(len(d), 5)
        for ts, v, dt in d:
            self.assertAlmostEqual(v, 6.5, 4)

        r = db.timeseries.get_single_timeseries("sensor1", "temp", t + 24 * 60 * 60, t + 24 * 60 * 60 + 500*600)
        a = list(r.all())
        d = list(r.aggregation("daily", "mean"))
        self.assertEqual(len(a), 501)
        self.assertLessEqual(len(d), 5)
        for ts, v, dt in d:
            self.assertAlmostEqual(v, 25.5, 4)

        s = db.timeseries.get_last_values("sensor1", ["temp", "ph"])
        self.assertEqual(len(s), 2)
        temp = s[0]
        self.assertEqual(len(temp), 1)
        self.assertEqual(temp[0].ts, t + 501 * 600 + 24 * 60 * 60)
        ph = s[1]
        self.assertEqual(len(ph), 1)
        self.assertEqual(ph[0].ts, t + 501 * 600)

        res = db.timeseries.get_full_timeseries("sensor1")
        self.assertEqual(len(res), 3)
        self.assertEqual(len(res[0]), 502)
        self.assertEqual(len(res[1]), 502)
        self.assertEqual(len(res[2]), 502)
        metrics = [x.metric for x in res]
        self.assertIn("temp", metrics)
        self.assertIn("act", metrics)
        self.assertIn("ph", metrics)

        r = FastDictTimeseries.from_float_timeseries(*res)
        self.assertEqual(len(r), 502+144)
        self.assertEqual(len(r[0].value), 2)
        self.assertIn("ph", r[150].value)
        self.assertIn("temp", r[150].value)
        self.assertIn("act", r[150].value)
        self.assertEqual(len(r[len(r)-1].value), 1)

    def test_delete(self):
        conf = get_unit_test_config()
        db = Connection(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                        metric_definitions=get_test_metrics())
        db.database_init(silent=True)
        db.timeseries._create_metric("ph", silent=True)

        base = datetime.datetime.now()
        data_list = [(base - datetime.timedelta(minutes=10*x), random.random() * 5) for x in range(0, 144*5)]
        ts = TimeSeries("device", "ph", values=data_list)
        from_pd = pendulum.instance(data_list[-1][0])
        from_ts = from_pd.int_timestamp
        to_pd = pendulum.instance(data_list[0][0])
        to_ts = to_pd.int_timestamp

        #delete all data just in case
        r = db.timeseries.delete_timeseries("device", ["ph"], from_ts-24*60*60, to_ts+24*60*60)

        #insert
        db.timeseries.insert_timeseries(ts)

        # get
        r = db.timeseries.get_single_timeseries("device", "ph", from_ts, to_ts)
        a = list(r.all())
        self.assertEqual(len(a), 144 * 5)

        # perform delete
        r = db.timeseries.delete_timeseries("device", ["ph"], from_ts, from_ts)
        self.assertEqual(r, 1)

        # get
        r = db.timeseries.get_single_timeseries("device", "ph", from_ts + 24*60*60, to_ts + 24*60*60)
        a = list(r.all())
        self.assertEqual(len(a), 144 * 4)

        # delete all
        r = db.timeseries.delete_timeseries("device", ["ph"], from_ts, to_ts)
        self.assertGreaterEqual(r, 5)

    def test_signal(self):
        conf = get_unit_test_config()
        db = Connection(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                        metric_definitions=get_test_metrics())
        db.database_init(silent=True)

        d = [[int(time.time()), 11.1]]
        data = [{"key": "sensor15",
                 "metric": "ph",
                 "data": d}]

        from blinker import signal
        my_put_func = mock.MagicMock(spec={})
        s = signal("timeseries.put")
        s.connect(my_put_func)
        from blinker import signal
        my_get_func = mock.MagicMock(spec={})
        s = signal("timeseries.get")
        s.connect(my_get_func)

        db.timeseries.insert_bulk(data)
        r = db.timeseries.get_single_timeseries("sensor15", "ph", 0, 500*600-1)

        self.assertEqual(len(my_put_func.call_args_list), 1)
        self.assertIn("info", my_put_func.call_args_list[0][1])

    def test_large(self):
        conf = get_unit_test_config()
        db = Connection(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                        metric_definitions=get_test_metrics())
        db.database_init(silent=True)

        start = 1483272000

        for id in ["sensor41", "sensor45", "sensor23", "sensor47"]:
            d1 = [(start + i * 600, 6.5) for i in range(5000)]
            d2 = [(start + i * 600, 10.5) for i in range(5000)]
            d3 = [(start, 20.43)]

            data = [{"key": id,
                    "metric": "act",
                    "data": d1},
                    {"key": id,
                    "metric": "temp",
                    "data": d2},
                    {"key": id,
                    "metric": "ph",
                    "data": d3}]
            db.timeseries.insert_bulk(data)

        r = db.timeseries.get_timeseries("sensor47", ["act", "temp", "ph"], start, start+600*4999)
        self.assertEqual(len(r[0]), 5000)
        self.assertEqual(len(r[1]), 5000)
        self.assertEqual(len(r[2]), 1)

        s = db.timeseries.get_last_values("sensor47", ["act", "temp", "ph"])
        act = s[0]
        self.assertEqual(act[0].ts, start + 600 * 4999)
        temp = s[1]
        self.assertEqual(temp[0].ts, start + 600 * 4999)
        ph = s[2]
        self.assertEqual(ph[0].ts, start)

    def test_selective_delete(self):
        conf = get_unit_test_config()
        db = Connection(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                        metric_definitions=get_test_metrics())
        db.database_init(silent=True)

        base = datetime.datetime(2019, 2, 1, 23, 50, tzinfo=datetime.timezone.utc)
        ph_data = [(base - datetime.timedelta(minutes=10*x),  ((x % 3) + 4)) for x in range(0, 144*5)]
        act_data = [(base - datetime.timedelta(minutes=10*x),  ((x % 3) + 20)) for x in range(0, 144*5)]
        ph = TimeSeries("dev1", "ph", values=ph_data)
        act = TimeSeries("dev1", "act", values=act_data)

        from_pd = pendulum.instance(ph_data[-1][0])
        from_ts = from_pd.int_timestamp
        to_pd = pendulum.instance(ph_data[0][0])
        to_ts = to_pd.int_timestamp

        #delete all data just in case
        r = db.timeseries.delete_timeseries("dev1", ["act", "ph"], from_ts-24*60*60, to_ts+24*60*60)
        db.timeseries.insert_timeseries(act)
        db.timeseries.insert_timeseries(ph)

        get_timeseries = db.timeseries.get_single_timeseries
        self.assertEqual(len(get_timeseries("dev1", "ph", from_ts, to_ts)), 144*5)
        self.assertEqual(len(get_timeseries("dev1", "act", from_ts, to_ts)), 144*5)
        # perform delete
        r = db.timeseries.delete_timeseries("dev1", ["ph"], from_ts, from_ts)
        self.assertEqual(r, 1)

        self.assertEqual(len(get_timeseries("dev1", "ph", from_ts, from_ts + 24*60*60 - 1)), 0)
        self.assertEqual(len(get_timeseries("dev1", "ph", from_ts, to_ts)), 144*4)
        self.assertEqual(len(get_timeseries("dev1", "act", from_ts, to_ts)), 144*5)

        delete_start = from_ts + 24*60*60
        delete_end = from_ts + 24*60*60*3

        r = db.timeseries.delete_timeseries("dev1", ["act"], delete_start + 12*60*60, delete_end - 12*60*60)
        self.assertEqual(r, 2)
        self.assertEqual(len(get_timeseries("dev1", "ph", from_ts, from_ts + 24*60*60 - 1)), 0)
        self.assertEqual(len(get_timeseries("dev1", "ph", from_ts, to_ts)), 144*4)
        self.assertEqual(len(get_timeseries("dev1", "act", from_ts, to_ts)), 144*3)
        self.assertEqual(len(get_timeseries("dev1", "act", from_ts, delete_start)), 144)
        self.assertEqual(len(get_timeseries("dev1", "act", delete_start, delete_end - 1)), 0)
        self.assertEqual(len(get_timeseries("dev1", "act", delete_end, to_ts)), 144*2)
