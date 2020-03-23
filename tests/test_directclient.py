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


from cattledb.directclient import CDBClient, create_client
from cattledb.storage.models import TimeSeries
from .helper import get_unit_test_config, get_test_metrics


class DirectclientTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpClass(cls):
        pass

    def test_base(self):
        client = create_client(get_unit_test_config())
        self.assertTrue(client.db)

    def test_timeseries(self):
        conf = get_unit_test_config()
        client = CDBClient(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                           table_prefix="mytestdb", read_only=False, admin=True)
        # Setup
        db = client.db
        db.add_metric_definitions(get_test_metrics())

        db.database_init(silent=True)
        db.timeseries._create_metric("ph", silent=True)
        db.timeseries._create_metric("act", silent=True)
        db.timeseries._create_metric("temp", silent=True)

        t = int(time.time() - 50 * 24 * 60 * 60)

        r = client.delete_timeseries("sensor1", ["ph", "act", "temp"], t, t + 500*600 + 24 * 60 * 60)

        d1 = [(t + i * 600, 6.5) for i in range(100)]
        d2 = [(t + i * 600, 25.5) for i in range(50)]

        data = [{"key": "sensor1",
                 "metric": "ph",
                 "data": d1},
                {"key": "sensor1",
                 "metric": "temp",
                 "data": d2}]
        res = client.put_timeseries_multi(data)
        self.assertEqual(res[0], 100)
        self.assertEqual(res[1], 50)


        r = client.get_timeseries("sensor1", ["ph", "temp"], t, t + 70*600-1)
        self.assertEqual(len(r[0]), 70)
        self.assertEqual(len(r[1]), 50)

        r = client.get_last_values("sensor1", ["ph", "temp"])
        self.assertEqual(r[0].last.ts,  t + 99 * 600)
        self.assertEqual(r[1].last.ts,  t + 49 * 600)
        self.assertEqual(r[0].first.value,  6.5)
        self.assertEqual(r[1].first.value,  25.5)

        r = client.get_all_metrics("sensor1", t, t + 70*600-1)
        counter = 0
        for row in r.yield_rows():
            self.assertEqual(len(row), 3)
            counter += 1
        self.assertEqual(counter, 70)

    def test_events(self):
        conf = get_unit_test_config()
        client = CDBClient(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                           table_prefix="mytestdb", read_only=False, admin=True)
        # Setup
        db = client.db
        db.database_init(silent=True)

        res = client.delete_events("device1", "upload", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC'),
                                                        pendulum.datetime(2015, 2, 7, 17, 0, tz='UTC'))
        self.assertTrue(res)

        events = [
            (pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, {"foo2": "bar2"}),
            (pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo4": "bar4"}),
            (pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"}),
            (pendulum.datetime(2015, 2, 5, 18, 0, tz='UTC').int_timestamp, {"foo3": "bar3"})
        ]
        res = client.put_events("device1", "upload", events)
        self.assertEqual(res, 4)

        res = client.get_events("device1", "upload", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC'),
                                                     pendulum.datetime(2015, 2, 5, 17, 0, tz='UTC'))
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].ts, pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].value["foo1"], "bar1")
        self.assertEqual(res[1].ts, pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[1].value["foo2"], "bar2")

        res = client.get_last_events("device1", "upload")
        self.assertEqual(res.name, "upload")
        self.assertEqual(res.key, "device1")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].ts, pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].value["foo4"], "bar4")

    def test_metadata(self):
        conf = get_unit_test_config()
        client = CDBClient(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                           table_prefix="mytestdb", read_only=False, admin=True)
        # Setup
        db = client.db
        db.database_init(silent=True)

        client.put_metadata("reader", "1", "note1", {"foo": "bar"})
        res = client.put_metadata("reader", "1", "note2", {"föö": "bää"})
        self.assertEqual(res, 1)

        res = db.metadata.get_metadata("reader", "1")
        self.assertEqual(len(res), 2)
        res = db.metadata.get_metadata("reader", "1", ["note1"])
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].data, {"foo": "bar"})

    def test_activity(self):
        conf = get_unit_test_config()
        client = CDBClient(engine=conf.ENGINE, engine_options=conf.ENGINE_OPTIONS,
                           table_prefix="mytestdb", read_only=False, admin=True)
        # Setup
        db = client.db
        db.database_init(silent=True)

        client.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        client.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        client.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 15, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        client.incr_activity("reader2", "dev2", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        client.incr_activity("reader2", "dev2", pendulum.datetime(2018, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        client.incr_activity("reader2", "dev1", pendulum.datetime(2018, 2, 4, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"], value=10)

        res = client.get_total_activity(pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].day_hour, "2018020512")
        self.assertEqual(res[0].reader_id, "reader1")
        self.assertEqual(res[0].device_ids, ["dev1"])

        res = client.get_day_activity("parent2", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].day_hour, "2018020512")
        self.assertEqual(res[0].reader_id, "reader1")
        self.assertEqual(res[1].day_hour, "2018020512")
        self.assertEqual(res[1].reader_id, "reader2")

        res = client.get_reader_activity("reader2", pendulum.datetime(2018, 2, 4, 12, 0, tz='UTC').int_timestamp, pendulum.datetime(2018, 2, 5, 15, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].device_id, "dev1")
        self.assertEqual(res[0].day_hour, "2018020412")
        self.assertEqual(res[1].device_id, "dev2")
        self.assertEqual(res[1].day_hour, "2018020512")
        self.assertEqual(res[2].device_id, "dev2")
        self.assertEqual(res[2].day_hour, "2018020513")