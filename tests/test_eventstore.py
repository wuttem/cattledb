#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import pendulum
import os
import datetime
import mock


from cattledb.storage.connection import Connection
from cattledb.storage.models import EventList
from cattledb.settings import EVENT_TYPES

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
        print(res)
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

    def test_daily_monthly(self):
        db = Connection(project_id='test-system', instance_id='test', event_definitions=EVENT_TYPES)
        db.create_tables(silent=True)

        db.store_event_definitions()
        db.load_event_definitions()
        self.assertEqual(len(EVENT_TYPES), len(db.event_definitions))

        db.events.insert_event("device1", "test_daily", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_daily", pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_daily", pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})

        db.events.insert_event("device1", "test_monthly", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_monthly", pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_monthly", pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})

        db.events.insert_event("device1", "test_monthly_2", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_monthly_2", pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_monthly_2", pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})

        db.events.insert_event("device1", "test_default", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_default", pendulum.datetime(2015, 2, 6, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})
        db.events.insert_event("device1", "test_default", pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp, {"foo1": "bar1"})

        from blinker import signal
        my_get_func = mock.MagicMock(spec={})
        s = signal("event.get")
        s.connect(my_get_func)

        res = db.events.get_events("device1", "test_daily", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(my_get_func.call_args_list), 1)
        self.assertEqual(my_get_func.call_args_list[0][1]["info"]["count"], 3)
        self.assertEqual(my_get_func.call_args_list[0][1]["info"]["row_keys"][0], "device1#test_daily#29854845")

        res = db.events.get_events("device1", "test_monthly", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(my_get_func.call_args_list), 2)
        self.assertEqual(my_get_func.call_args_list[1][1]["info"]["count"], 1)
        self.assertEqual(my_get_func.call_args_list[1][1]["info"]["row_keys"][0], "device1#m_test_monthly#298548")

        res = db.events.get_events("device1", "test_default", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(my_get_func.call_args_list), 3)
        self.assertEqual(my_get_func.call_args_list[2][1]["info"]["count"], 3)
        self.assertEqual(my_get_func.call_args_list[2][1]["info"]["row_keys"][0], "device1#test_default#29854845")

        res = db.events.get_events("device1", "test_monthly_2", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp,
                                   pendulum.datetime(2015, 2, 7, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(len(my_get_func.call_args_list), 4)
        self.assertEqual(my_get_func.call_args_list[3][1]["info"]["count"], 1)
        self.assertEqual(my_get_func.call_args_list[3][1]["info"]["row_keys"][0], "device1#m_test_monthly_2#298548")