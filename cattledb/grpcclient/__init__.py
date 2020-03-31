#!/usr/bin/python
# coding: utf-8

import os
import grpc

from ..grpcserver import cdb_pb2 as cdb_pb2
from ..grpcserver import cdb_pb2_grpc as cdb_pb2_grpc
from ..storage.models import TimeSeries, EventList, SerializableNamespaceDict, ReaderActivityItem, DeviceActivityItem


class CDBClient(object):
    def __init__(self, endpoint=None, read_only=False, options=None):
        self.read_only = read_only
        self.options = options
        if endpoint is not None:
            self.setup(endpoint, read_only, options)
        #self.channel = grpc.insecure_channel(endpoint)
        #self.setup()
        #self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
        #self.events = cdb_pb2_grpc.EventsStub(self.channel)
        #self.metadata = cdb_pb2_grpc.MetaDataStub(self.channel)
        #self.activity = cdb_pb2_grpc.ActivityStub(self.channel)

    def setup(self, endpoint, read_only=False, options=None):
        self.read_only = read_only
        if options is not None:
            self.channel = grpc.insecure_channel(endpoint)
        else:
            self.channel = grpc.insecure_channel(endpoint, options)
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            raise RuntimeError('Error connecting to server: {}'.format(endpoint))
        else:
            self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
            self.events = cdb_pb2_grpc.EventsStub(self.channel)
            self.metadata = cdb_pb2_grpc.MetaDataStub(self.channel)
            self.activity = cdb_pb2_grpc.ActivityStub(self.channel)

    def raise_on_read_only(self):
        if self.read_only:
            raise RuntimeError("not possible in read only mode")

    # --------------------------------------------------------------------------
    # Timeseries
    # --------------------------------------------------------------------------

    def get_timeseries(self, key, metrics, from_datetime, to_datetime):
        req = cdb_pb2.MultiTimeSeriesRequest(key=key, metrics=metrics,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        ts = self.timeseries.getMulti(req)
        out = []
        for t in ts.data:
            try:
                out.append(TimeSeries.from_proto(t))
            except ValueError:
                # add empty timeseries
                out.append(TimeSeries(t.key, t.metric))
        return out

    def delete_timeseries(self, key, metrics, from_datetime, to_datetime):
        self.raise_on_read_only()
        req = cdb_pb2.MultiTimeSeriesRequest(key=key, metrics=metrics,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        res = self.timeseries.delete(req)
        return int(res.counter)

    def get_last_values(self, key, metrics):
        req = cdb_pb2.LastValuesRequest(key=key, metrics=metrics)
        ts = self.timeseries.lastValues(req)
        out = []
        for t in ts.data:
            try:
                out.append(TimeSeries.from_proto(t))
            except ValueError:
                # add empty timeseries
                out.append(TimeSeries(t.key, t.metric))
        return out

    def put_timeseries(self, key, metric, data):
        self.raise_on_read_only()
        ts = TimeSeries(key, metric, values=data)
        pb = ts.to_proto()
        res = self.timeseries.put(pb)
        return int(res.counter)

    def put_timeseries_multi(self, data):
        self.raise_on_read_only()
        ts_list = []
        for item in data:
            key = item["key"]
            metric = item["metric"]
            data = item["data"]
            ts = TimeSeries(key, metric, values=data)
            pb = ts.to_proto()
            ts_list.append(pb)
        l = cdb_pb2.FloatTimeSeriesList()
        l.data.extend(ts_list)
        res = self.timeseries.putMulti(l)
        return int(res.counter)

    # --------------------------------------------------------------------------
    # Events
    # --------------------------------------------------------------------------

    def put_events(self, key, name, events):
        self.raise_on_read_only()
        ev = EventList(key, name, events)
        pb = ev.to_proto()
        res = self.events.put(pb)
        return int(res.counter)

    def get_events(self, key, name, from_datetime, to_datetime):
        req = cdb_pb2.EventsRequest(key=key, name=name,
                                    from_datetime=from_datetime.isoformat(),
                                    to_datetime=to_datetime.isoformat())
        ts = self.events.get(req)
        return EventList.from_proto(ts)

    def get_last_events(self, key, name):
        req = cdb_pb2.LastEventsRequest(key=key, name=name)
        ts = self.events.lastEvents(req)
        return EventList.from_proto(ts)

    def delete_events(self, key, name, from_datetime, to_datetime):
        self.raise_on_read_only()
        req = cdb_pb2.EventsRequest(key=key, name=name,
                                    from_datetime=from_datetime.isoformat(),
                                    to_datetime=to_datetime.isoformat())
        res = self.events.delete(req)
        return int(res.counter)

    # --------------------------------------------------------------------------
    # Metadata
    # --------------------------------------------------------------------------

    def put_metadata(self, object_name, object_key, namespace, data):
        self.raise_on_read_only()
        if not isinstance(data, dict):
            raise ValueError("data should be a dict")
        d = SerializableNamespaceDict(namespace, data)
        req = cdb_pb2.MetaDataPost(object_name=object_name, object_key=object_key, data=[d.to_proto()])
        res = self.metadata.put(req)
        return int(res.counter)

    def get_metadata(self, object_name, object_key, namespaces=None):
        req = cdb_pb2.MetaDataRequest(object_name=object_name, object_key=object_key,
                                      namespaces=namespaces)
        ts = self.metadata.get(req)
        res = {"object_name": ts.object_name, "object_key": object_key, "data": {}}
        for i in ts.data:
            sd = SerializableNamespaceDict.from_proto(i)
            res["data"][sd.namespace] = sd.to_dict()
        return res

    # --------------------------------------------------------------------------
    # Activity
    # --------------------------------------------------------------------------

    def incr_activity(self, reader_id, device_id, timestamp, parent_ids=None, value=1):
        self.raise_on_read_only()
        req = cdb_pb2.IncrementActivityRequest(reader_id=reader_id, device_id=device_id,
                                               timestamp=timestamp.isoformat(), value=value)
        if parent_ids is not None:
            req.parent_ids.extend(list(parent_ids))
        res = self.activity.increment(req)
        return int(res.counter)

    def get_total_activity(self, day):
        req = cdb_pb2.TotalActivityRequest(day_datetime=day.isoformat())
        ts = self.activity.getTotal(req)
        res = []
        for act in ts.activities:
            res.append(ReaderActivityItem.from_proto(act).to_dict())
        return res

    def get_day_activity(self, parent_id, day):
        req = cdb_pb2.ActivityDayRequest(day_datetime=day.isoformat(), parent_id=parent_id)
        ts = self.activity.getDay(req)
        res = []
        for act in ts.activities:
            res.append(ReaderActivityItem.from_proto(act).to_dict())
        return res

        return ts

    def get_reader_activity(self, reader_id, from_datetime, to_datetime):
        req = cdb_pb2.ReaderActivityRequest(reader_id=reader_id,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        ts = self.activity.getReader(req)
        res = []
        for act in ts.activities:
            res.append(DeviceActivityItem.from_proto(act).to_dict())
        return res