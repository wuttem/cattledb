#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage import Connection
from cattledb.storage.models import Event, EventList


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
            Event(pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, {"foo2": "bar2"}),
            Event(pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo4": "bar4"})
            ])
        db.events.insert_events(evs)

        res = db.events.get_events("device1", "upload", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 5, 17, 0, tz='UTC').int_timestamp)

        self.assertEqual(res.name, "upload")
        self.assertEqual(res.key, "device1")
        self.assertEqual(len(res.events), 2)
        self.assertEqual(res.events[0].ts, pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res.events[0].data["foo1"], "bar1")
        self.assertEqual(res.events[1].ts, pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp)
        self.assertEqual(res.events[1].data["foo2"], "bar2")
