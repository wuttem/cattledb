#!/usr/bin/python
# coding: utf8

import os
import grpc

from ..grpcserver import cdb_pb2 as cdb_pb2
from ..grpcserver import cdb_pb2_grpc as cdb_pb2_grpc
from ..storage.models import TimeSeries, EventList, SerializableNamespaceDict


class CDBClient(object):
    def __init__(self, endpoint):
        self.channel = grpc.insecure_channel(endpoint)
        self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
        self.events = cdb_pb2_grpc.EventsStub(self.channel)
        self.metadata = cdb_pb2_grpc.MetaDataStub(self.channel)
        self.activity = cdb_pb2_grpc.ActivityStub(self.channel)

    def setup(self):
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            raise RuntimeError('Error connecting to server')
        else:
            self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
            self.events = cdb_pb2_grpc.EventsStub(self.channel)
            self.metadata = cdb_pb2_grpc.MetaDataStub(self.channel)
            self.activity = cdb_pb2_grpc.ActivityStub(self.channel)

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
        req = cdb_pb2.MultiTimeSeriesRequest(key=key, metrics=metrics,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        res = self.timeseries.delete(req)
        return res

    def get_last_values(self, key, metrics, max_ts=None):
        if max_ts is not None:
            max_ts = max_ts.isoformat()
        req = cdb_pb2.LastValuesRequest(key=key, metrics=metrics,
                                        max_ts=max_ts)
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
        ts = TimeSeries(key, metric, values=data)
        pb = ts.to_proto()
        res = self.timeseries.put(pb)
        return res

    def put_timeseries_multi(self, data):
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
        return res

    # --------------------------------------------------------------------------
    # Events
    # --------------------------------------------------------------------------

    def put_events(self, key, name, events):
        ev = EventList(key, name, events)
        pb = ev.to_proto()
        res = self.events.put(pb)
        return res

    def get_events(self, key, name, from_datetime, to_datetime):
        req = cdb_pb2.EventsRequest(key=key, name=name,
                                    from_datetime=from_datetime.isoformat(),
                                    to_datetime=to_datetime.isoformat())
        ts = self.events.get(req)
        return EventList.from_proto(ts)

    def get_last_events(self, key, name, max_ts=None, count=1):
        if max_ts is not None:
            max_ts = max_ts.isoformat()
        req = cdb_pb2.LastEventsRequest(key=key, name=name,
                                        max_ts=max_ts, count=count)
        ts = self.events.lastEvents(req)
        return EventList.from_proto(ts)

    def delete_events(self, key, name, from_datetime, to_datetime):
        req = cdb_pb2.EventsRequest(key=key, name=name,
                                    from_datetime=from_datetime.isoformat(),
                                    to_datetime=to_datetime.isoformat())
        res = self.events.delete(req)
        return res

    # --------------------------------------------------------------------------
    # Metadata
    # --------------------------------------------------------------------------

    def put_metadata(self, object_name, object_key, namespace, data):
        d = SerializableNamespaceDict(namespace, data)
        req = cdb_pb2.MetaDataPost(object_name=object_name, object_key=object_key, data=[d.to_proto()])
        res = self.metadata.put(req)
        return res

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
        req = cdb_pb2.IncrementActivityRequest(reader_id=reader_id, device_id=device_id,
                                               timestamp=timestamp.isoformat(), value=value)
        if parent_ids is not None:
            req.parent_ids.extend(list(parent_ids))
        res = self.activity.increment(req)
        return res

    def get_total_activity(self, day):
        req = cdb_pb2.TotalActivityRequest(day_datetime=day.isoformat())
        ts = self.activity.getTotal(req)
        return ts

    def get_day_activity(self, parent_id, day):
        req = cdb_pb2.ActivityDayRequest(day_datetime=day.isoformat(), parent_id=parent_id)
        ts = self.activity.getDay(req)
        return ts

    def get_reader_activity(self, reader_id, from_datetime, to_datetime):
        req = cdb_pb2.ReaderActivityRequest(reader_id=reader_id,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        ts = self.activity.getReader(req)
        return ts