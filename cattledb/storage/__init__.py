#!/usr/bin/python
# coding: utf8


import logging
import asyncio
import concurrent.futures

from .connection import Connection as Connection


logger = logging.getLogger(__name__)


class AsyncDB(object):
    def __init__(self, project_id, instance_id, loop=None, read_only=False, pool_size=8, table_prefix="cdb",
                 credentials=None, metric_definition=None):
        self.db = Connection(project_id=project_id, instance_id=instance_id, read_only=read_only,
                             pool_size=pool_size, table_prefix=table_prefix, credentials=credentials,
                             metric_definition=metric_definition)

        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=pool_size*2)
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    async def put(self, key, metric, data):
        return await self.loop.run_in_executor(self.pool, self.db.insert, key, metric, data, True)

    async def delete(self, key, metrics, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.delete_timeseries, key, metrics, from_dt, to_dt)

    async def get(self, key, metrics, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.get_timeseries, key, metrics, from_dt, to_dt)

    async def get_last(self, key, metrics, count=1, max_days=180):
        return await self.loop.run_in_executor(self.pool, self.db.get_last_values, key, metrics, count, max_days)

    async def get_single(self, key, metric, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.get_single_timeseries, key, metric, from_dt, to_dt)