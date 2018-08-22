#!/usr/bin/python
# coding: utf8
from __future__ import unicode_literals

import unittest
import random
import logging
import pendulum
import os
import datetime


from cattledb.storage.connection import Connection
from cattledb.storage.models import RowUpsert


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
        db = Connection(project_id='test-system', instance_id='test')
        db.create_tables(silent=True)

        self.assertEqual(db.table_with_prefix("metadata"), "mycdb_metadata")

        db.write_cell("metadata", "abc123", "p:foo", "bär".encode("utf-8"))

        res = db.read_row("metadata", "abc123")
        self.assertEqual(res["p:foo"].decode("utf-8"), "bär")

    def test_rows(self):
        inserts = []
        inserts.append(RowUpsert("abc#1#1", {"p:k": b"11"}))
        inserts.append(RowUpsert("abc#1#2", {"p:k": b"12"}))
        inserts.append(RowUpsert("abc#2#1", {"p:k": b"21", "i:k": b"21"}))
        inserts.append(RowUpsert("abc#2#2", {"p:k": b"22", "i:k": b"22"}))
        inserts.append(RowUpsert("abc#2#3", {"p:k": b"23", "i:k": b"23"}))
        inserts.append(RowUpsert("abc#3#1", {"p:k": b"31"}))
        inserts.append(RowUpsert("abc#3#2", {"p:k": b"32"}))

        db = Connection(project_id='test-system', instance_id='test')
        db.create_tables(silent=True)
        table = db.metadata_table()
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
        self.assertEqual(len(res), 2)
        self.assertNotIn("p:k", res[0][1])
        self.assertIn("i:k", res[0][1])
        self.assertNotIn("p:k", res[1][1])
        self.assertNotIn("i:k", res[1][1])

        res = table.read_rows(prefix="abc#2", column_families=["p"])
        self.assertEqual(len(res), 3)
        self.assertIn("p:k", res[0][1])
        self.assertNotIn("i:k", res[0][1])

        res = table.read_rows(prefix="abc#2")
        self.assertEqual(len(res), 3)
        self.assertIn("p:k", res[0][1])
        self.assertIn("i:k", res[0][1])