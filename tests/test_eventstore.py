#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage import Connection
from cattledb.storage.models import EventList


class EventStorageTest(unittest.TestCase):
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
        # os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/mnt/c/Users/mths/.ssh/google_gcp_credentials.json"

    def test_simple(self):
        db = Connection(project_id='test-system', instance_id='test')
        db.create_tables(silent=True)

        db.events.insert_event("device1", "upload", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "upload", pendulum.datetime(2015, 2, 5, 18, 0, tz='UTC').int_timestamp, {"foo3": "bar3"})
        evs = EventList("device1", "upload", [
            (pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, {"foo2": "bar2"}),
            (pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo4": "bar4"})
            ])
        db.events.insert_events(evs)

        res = db.events.get_events("device1", "upload", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 5, 17, 0, tz='UTC').int_timestamp)

        self.assertEqual(res.name, "upload")
        self.assertEqual(res.key, "device1")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].ts, pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].value["foo1"], "bar1")
        self.assertEqual(res[1].ts, pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[1].value["foo2"], "bar2")

        res = db.events.get_last_event("device1", "upload")
        self.assertEqual(res.name, "upload")
        self.assertEqual(res.key, "device1")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].ts, pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].value["foo4"], "bar4")

        res = db.events.delete_event_days("device1", "upload", pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp)

        res = db.events.get_last_event("device1", "upload")
        self.assertEqual(res.name, "upload")
        self.assertEqual(res.key, "device1")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].ts, pendulum.datetime(2015, 2, 5, 18, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].value["foo3"], "bar3")