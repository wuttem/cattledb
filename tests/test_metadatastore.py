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


class MetaDataStorageTest(unittest.TestCase):
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

        db.metadata.put_metadata("reader", "1", "note1", {"foo": "bar"})
        db.metadata.put_metadata("reader", "1", "note2", {"föö": "bää"})

        res = db.metadata.get_metadata("reader", "1")
        self.assertEqual(len(res), 2)
