#!/usr/bin/python
# coding: utf8

import os
import grpc

from ..grpcserver import cdb_pb2 as cdb_pb2
from ..grpcserver import cdb_pb2_grpc as cdb_pb2_grpc
from ..storage.models import TimeSeries, EventList


class CDBClient(object):
    def __init__(self, endpoint):
        self.channel = grpc.insecure_channel(endpoint)
        self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
        self.events = cdb_pb2_grpc.EventsStub(self.channel)

    def setup(self):
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            raise RuntimeError('Error connecting to server')
        else:
            self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)
            self.events = cdb_pb2_grpc.EventsStub(self.channel)

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