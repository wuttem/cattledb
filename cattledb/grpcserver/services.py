import grpc
import pendulum
from pendulum.parsing.exceptions import ParserError
from .cdb_pb2 import FloatTimeSeries, FloatTimeSeriesList, PutResult
from .cdb_pb2_grpc import TimeSeriesServicer
from ..storage import Connection
from ..storage.models import TimeSeries

class TimeSeriesServicer(TimeSeriesServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, project_id, instance_id, read_only=False, pool_size=8, table_prefix="cdb", credentials=None):
        self.db = Connection(project_id=project_id, instance_id=instance_id, read_only=read_only,
                             pool_size=pool_size, table_prefix=table_prefix, credentials=credentials)


    def get(self, request, context):
        # request: TimeSeriesRequest
        # return: FloatTimeSeries

        if not request.key or not request.metric or not request.from_datetime or not request.to_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return FloatTimeSeries()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for from_datetime and to_datetime")
            return FloatTimeSeries()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        ts = self.db.timeseries.get_single_timeseries(request.key, request.metric, from_ts, to_ts)
        return ts.to_proto()

    def getMulti(self, request, context):
        # request: MultiTimeSeriesRequest
        # return: FloatTimeSeriesList

        if not request.key or not request.metrics or not request.from_datetime or not request.to_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return FloatTimeSeriesList()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for from_datetime and to_datetime")
            return FloatTimeSeriesList()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        ts_list = self.db.timeseries.get_timeseries(request.key, request.metrics, from_ts, to_ts)
        l = FloatTimeSeriesList()
        l.data.extend([r.to_proto() for r in ts_list])
        return l

    def putFloat(self, request, context):
        # request: FloatTimeSeries
        # return: PutResult

        if (not request.key or not request.metric or not request.timestamps
                or not request.values or not request.timestamp_offsets):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return PutResult()

        assert 2 <= len(request.metric) <= 64
        assert 3 <= len(request.key) <= 32
        assert len(request.timestamps) > 0
        assert len(request.values) == len(request.timestamps) == len(request.timestamp_offsets)

        ts = TimeSeries.from_proto(request)
        res = self.db.timeseries.insert_timeseries(ts)
        return PutResult(code=200, counter=int(res), message="success")

    def putMulti(self, request, context):
        # request: FloatTimeSeriesList
        # return: PutResult
        pass