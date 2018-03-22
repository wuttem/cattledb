#!/usr/bin/python
# coding: utf8

import asyncio
import concurrent.futures

from ..storage import Connection

class DBAdapter(object):
    def __init__(self, app=None):
        self._db = None
        self.loop = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app, loop=None):
        self._db = Connection(project_id='test-system', instance_id='test')
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=40)
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    @property
    def db(self):
        return self._db

    def getTest(self):
        return 1

    async def put(self, device_id, metric, data):
        return await self.loop.run_in_executor(self.pool, self.db.insert, device_id, metric, data, True)
        #return self.db.insert(device_id, metric, data, force_float=True)

    def get(self, device_id, metric, from_dt, to_dt):
        return self.db.get_single_timeseries(device_id, metric, from_dt, to_dt)