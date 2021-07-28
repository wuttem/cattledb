#!/usr/bin/python
# coding: utf-8

import unittest
import random
import logging
from google.cloud.bigtable import column_family
import pendulum
import os
import datetime


from cattledb.storage.connection import Connection
from cattledb.storage.models import RowUpsert

from .helper import get_unit_test_config, get_test_metrics

class LocalSQLTest(unittest.TestCase):
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

    def test_base(self):
        db = Connection(engine="dynamo", engine_options={"emulator": "localhost:8000"})
        db.database_init(silent=True)

        db.write_cell("metadata", "abc123", "p:foo", "bär".encode("utf-8"))

        res = db.read_row("metadata", "abc123", column_families=["p"])
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

        db = Connection(engine="dynamo", engine_options={"emulator": "localhost:8000"})
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

    def test_sorted_rows(self):
        inserts = []
        inserts.append(RowUpsert("abc#1#1", {"p:k": b"11"}))
        inserts.append(RowUpsert("abc#1#2", {"p:k": b"12"}))
        inserts.append(RowUpsert("abc#1#3", {"p:k": b"13", "i:k": b"13"}))
        inserts.append(RowUpsert("abc#2#2", {"p:k": b"22", "i:k": b"22"}))
        inserts.append(RowUpsert("abc#2#3", {"p:k": b"23", "i:k": b"23"}))
        inserts.append(RowUpsert("abc#2#1", {"p:k": b"21"}))
        inserts.append(RowUpsert("abc#3#10", {"p:k": b"310"}))

        db = Connection(engine="dynamo", engine_options={"emulator": "localhost:8000"})
        db.database_init(silent=True)
        table = db.events.table()
        table.upsert_rows(inserts)

        res = table.get_first_row("abc#1#", column_families=["i"])
        self.assertEqual(res[0], "abc#1#3")
        self.assertEqual(res[1]["i:k"], b"13")

        res = table.get_first_row("abc#1#", column_families=["p"])
        self.assertEqual(res[0], "abc#1#1")
        self.assertEqual(res[1]["p:k"], b"11")

        res = table.get_first_row("abc#", column_families=["p"])
        self.assertEqual(res, None)

    def test_schema(self):
        db = Connection(engine="dynamo", engine_options={"emulator": "localhost:8000"})
        db.database_init(silent=True)

        res = db.read_database_structure()
        assert len(res) == 5

    def test_large(self):
        conf = get_unit_test_config("dynamo")
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