#!/usr/bin/python
# coding: utf-8

import unittest
import random
import logging
import pendulum
import os
import datetime
import asyncio


from cattledb.directclient import AsyncCDBClient
from cattledb.settings import AVAILABLE_METRICS, UnitTestConfig


class AsyncTest(unittest.TestCase):
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
        client = AsyncCDBClient(engine='bigtable', engine_options=UnitTestConfig.ENGINE_OPTIONS,
                                table_prefix="mytestdb", read_only=False)

        # setup
        db = client._client.db
        db.database_init(silent=True)

        loop = asyncio.get_event_loop()

        res = loop.run_until_complete(client.put_metadata("object", "id1", "note2", {"föö": "bää"}))
        self.assertEqual(res, 1)

        res = loop.run_until_complete(client.get_metadata("object", "id1", ["note2"]))
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].data, {"föö": "bää"})
