#!/usr/bin/python
# coding: utf8

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage import Connection


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
        db = Connection(project_id='test-system', instance_id='test')
        db.create_tables(silent=True)

        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader1", "dev1", pendulum.datetime(2015, 2, 5, 15, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader2", "dev2", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"])
        db.activity.incr_activity("reader2", "dev2", pendulum.datetime(2015, 2, 5, 13, 0, tz='UTC').int_timestamp, parent_ids=["parent1"])
        db.activity.incr_activity("reader2", "dev1", pendulum.datetime(2015, 2, 4, 12, 0, tz='UTC').int_timestamp, parent_ids=["parent1", "parent2"], value=10)

        res = db.activity.get_total_activity_for_day(pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0][0], "2015020512")
        self.assertEqual(res[0][1]["reader1"], ["dev1"])
        self.assertEqual(res[0][1]["reader2"], ["dev2"])
        self.assertEqual(res[1][0], "2015020513")
        self.assertEqual(res[1][1]["reader1"], ["dev1"])
        self.assertEqual(res[1][1]["reader2"], ["dev2"])
        self.assertEqual(res[2][0], "2015020515")

        res = db.activity.get_activity_for_day("parent2", pendulum.datetime(2015, 2, 5, 12, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], "2015020512")
        self.assertEqual(res[0][1]["reader1"], ["dev1"])
        self.assertEqual(res[0][1]["reader2"], ["dev2"])

        res = db.activity.get_activity_for_reader("reader2", pendulum.datetime(2015, 2, 4, 12, 0, tz='UTC').int_timestamp, pendulum.datetime(2015, 2, 5, 15, 0, tz='UTC').int_timestamp)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0][0], "2015020412")
        self.assertIn("dev1", res[0][1])
        self.assertEqual(res[1][0], "2015020512")
        self.assertIn("dev2", res[1][1])
        self.assertEqual(res[2][0], "2015020513")
        self.assertIn("dev2", res[2][1])

