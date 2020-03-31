#!/usr/bin/python
# coding: utf-8

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage.connection import Connection
from cattledb.storage.models import RowUpsert
from .helper import get_unit_test_config, get_test_connection


class ConnectionTest(unittest.TestCase):
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

    def test_base(self):
        db = get_test_connection()
        db.database_init(silent=True)

        db.write_cell("metadata", "abc123", "p:foo", "bär".encode("utf-8"))
        res = db.read_row("metadata", "abc123")
        self.assertEqual(res["p:foo"].decode("utf-8"), "bär")

        db.write_config("config_key_1", [1, 4, "föo"])
        conf = db.read_config("config_key_1")
        self.assertEqual(conf, [1, 4, "föo"])

        res = db.read_database_structure()
        assert len(res) == 5

    def test_rows(self):
        inserts = []
        inserts.append(RowUpsert("abc#1#1", {"p:k": b"11"}))
        inserts.append(RowUpsert("abc#1#2", {"p:k": b"12"}))
        inserts.append(RowUpsert("abc#2#1", {"p:k": b"21", "i:k": b"21"}))
        inserts.append(RowUpsert("abc#2#2", {"p:k": b"22", "i:k": b"22"}))
        inserts.append(RowUpsert("abc#2#3", {"p:k": b"23", "i:k": b"23"}))
        inserts.append(RowUpsert("abc#3#1", {"p:k": b"31"}))
        inserts.append(RowUpsert("abc#3#2", {"p:k": b"32"}))

        db = get_test_connection()
        db.database_init(silent=True)
        table = db.metadata.table()
        table.upsert_rows(inserts)

        res = table.read_rows(row_keys=["abc#2#1", "abc#3#1"], column_families=["p"])
        self.assertEqual(len(res), 2)
        self.assertIn("p:k", res[0][1])
        self.assertNotIn("i:k", res[0][1])
        self.assertIn("p:k", res[1][1])
        self.assertNotIn("i:k", res[1][1])

        res = table.read_rows(row_keys=["abc#2#1", "abc#3#1"], column_families=["p", "i"])
        self.assertEqual(len(res), 2)
        self.assertIn("p:k", res[0][1])
        self.assertIn("i:k", res[0][1])
        self.assertIn("p:k", res[1][1])

        res = table.read_rows(row_keys=["abc#2#1", "abc#3#1"], column_families=["i"])
        self.assertEqual(len(res), 1)
        self.assertNotIn("p:k", res[0][1])
        self.assertIn("i:k", res[0][1])

        res = table.get_first_row("abc#", column_families=["i"])
        self.assertEqual(res[0], "abc#2#1")
        self.assertEqual(res[1]["i:k"], b"21")

        res = table.get_first_row("abc#3", column_families=["p"])
        self.assertEqual(res[0], "abc#3#1")
        self.assertEqual(res[1]["p:k"], b"31")

        res = table.get_first_row("abc#3", column_families=["i"])
        self.assertEqual(res, None)

        res = table.read_rows(start_key="abc#2", end_key="abc#3#2")
        self.assertEqual(len(res), 5)

        res = table.read_rows(start_key="abc#2", end_key="abc#3#2", column_families=["i"])
        self.assertEqual(len(res), 3)
