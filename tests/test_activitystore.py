#!/usr/bin/python
# coding: utf-8

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage.connection import Connection
from .helper import get_unit_test_config, get_test_connection


class ActivityStorageTest(unittest.TestCase):
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
        db = get_test_connection()
        db.database_init(silent=True)

        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2018, 2, 5, 15, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader2", "dev2", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        db.activity.incr_activity("reader2", "dev2", pendulum.datetime(2018, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader2", "dev1", pendulum.datetime(2018, 2, 4, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"], value=10)

        res = db.activity.get_total_activity_for_day(pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(res[0].day_hour, "2018020512")
        self.assertEqual(res[0].reader_id, "reader1")
        self.assertEqual(res[0].device_ids, ["dev1"])

        res = db.activity.get_activity_for_day("parent2", pendulum.datetime(2018, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].day_hour, "2018020512")
        self.assertEqual(res[0].reader_id, "reader1")
        self.assertEqual(res[1].day_hour, "2018020512")
        self.assertEqual(res[1].reader_id, "reader2")

        res = db.activity.get_activity_for_reader("reader2", pendulum.datetime(2018, 2, 4, 12, 0, tz='UTC').int_timestamp, pendulum.datetime(2018, 2, 5, 15, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].device_id, "dev1")
        self.assertEqual(res[0].day_hour, "2018020412")
        self.assertEqual(res[1].device_id, "dev2")
        self.assertEqual(res[1].day_hour, "2018020512")
        self.assertEqual(res[2].device_id, "dev2")
        self.assertEqual(res[2].day_hour, "2018020513")
