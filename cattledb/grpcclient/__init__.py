#!/usr/bin/python
# coding: utf8

import os
import grpc

from ..grpcserver import cdb_pb2 as cdb_pb2
from ..grpcserver import cdb_pb2_grpc as cdb_pb2_grpc
from ..storage.models import TimeSeries


class CDBClient(object):
    def __init__(self, endpoint):
        self.channel = grpc.insecure_channel(endpoint)
        self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)

    def setup(self):
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            raise RuntimeError('Error connecting to server')
        else:
            self.timeseries = cdb_pb2_grpc.TimeSeriesStub(self.channel)

    def make_request(self, key, metrics, from_datetime, to_datetime):
        return cdb_pb2.MultiTimeSeriesRequest(key=key, metrics=metrics,
                                              from_datetime=from_datetime, to_datetime=to_datetime)

    def get_timeseries(self, key, metrics, from_datetime, to_datetime):
        req = cdb_pb2.MultiTimeSeriesRequest(key=key, metrics=metrics,
                                            from_datetime=from_datetime.isoformat(),
                                            to_datetime=to_datetime.isoformat())
        ts = self.timeseries.getMultiFloat(req)
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
        res = self.timeseries.putFloat(pb)
        return res