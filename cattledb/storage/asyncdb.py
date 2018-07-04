#!/usr/bin/python
# coding: utf8


import logging
import asyncio
import concurrent.futures


logger = logging.getLogger(__name__)


class AsyncDB(object):
    def __init__(self, project_id, instance_id, loop=None, read_only=False, pool_size=8, table_prefix="cdb",
                 credentials=None, metric_definition=None):
        from .connection import Connection as Connection
        self.db = Connection(project_id=project_id, instance_id=instance_id, read_only=read_only,
                             pool_size=pool_size, table_prefix=table_prefix, credentials=credentials,
                             metric_definition=metric_definition)

        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=pool_size*2)
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

    # Timeseries

    async def put_timeseries(self, ts):
        return await self.loop.run_in_executor(self.pool, self.db.timeseries.insert_timeseries, ts)

    async def delete_timeseries(self, key, metrics, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.timeseries.delete_timeseries, key, metrics, from_dt, to_dt)

    async def get_timeseries(self, key, metrics, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.timeseries.get_timeseries, key, metrics, from_dt, to_dt)

    async def get_last_timeseries(self, key, metrics, count=1, max_days=180):
        return await self.loop.run_in_executor(self.pool, self.db.timeseries.get_last_values, key, metrics, count, max_days)


    # Activity

    async def increment_activity(self, reader_id, device_id, timestamp, parent_ids=None, value=1):
        return await self.loop.run_in_executor(self.pool, self.db.activity.incr_activity, device_id, timestamp, parent_ids, value)

    async def get_reader_activity(self, reader_id, from_ts, to_ts):
        return await self.loop.run_in_executor(self.pool, self.db.activity.get_activity_for_reader, reader_id, from_ts, to_ts)

    async def get_total_activity(self, day_ts):
        return await self.loop.run_in_executor(self.pool, self.db.activity.get_total_activity_for_day, day_ts)

    async def get_day_activity(self, parent_id, day_ts):
        return await self.loop.run_in_executor(self.pool, self.db.activity.get_activity_for_day, parent_id, day_ts)

    # Events

    async def put_events(self, event_list):
        return await self.loop.run_in_executor(self.pool, self.db.events.insert_events, event_list)

    async def delete_events(self, key, name, from_ts, to_ts):
        return await self.loop.run_in_executor(self.pool, self.db.events.delete_event_days, key, name, from_ts, to_ts)

    async def get_events(self, key, metrics, from_dt, to_dt):
        return await self.loop.run_in_executor(self.pool, self.db.events.get_timeseries, key, metrics, from_dt, to_dt)

    async def get_last_event(self, key, name, count=1, max_days=180):
        return await self.loop.run_in_executor(self.pool, self.db.events.get_last_events, key, name, count, max_days)

    # Metadata

    async def put_metadata(self, object_name, object_id, key, data, internal=False):
        return await self.loop.run_in_executor(self.pool, self.db.metadata.put_metadata, object_name, object_id, key, data, internal)

    async def get_metadata(self, object_name, object_id, keys=None, internal=False):
        return await self.loop.run_in_executor(self.pool, self.db.events.get_metadata, object_name, object_id, keys, internal)