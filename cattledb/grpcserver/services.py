import grpc
import pendulum
from pendulum.parsing.exceptions import ParserError
from .cdb_pb2 import FloatTimeSeries, FloatTimeSeriesList, PutResult, DeleteResult, EventSeries
from .cdb_pb2_grpc import TimeSeriesServicer, ActivityServicer, MetaDataServicer, EventsServicer
from ..storage.models import TimeSeries, EventList

class TimeSeriesServicer(TimeSeriesServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, db_instance):
        self.db = db_instance

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

    def put(self, request, context):
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

        for p in request.data:
            if (not p.key or not p.metric or not p.timestamps
                or not p.values or not p.timestamp_offsets):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Invalid Request')
                return PutResult()

            assert 2 <= len(p.metric) <= 64
            assert 3 <= len(p.key) <= 32
            assert len(p.timestamps) > 0
            assert len(p.values) == len(p.timestamps) == len(p.timestamp_offsets)

        res_counter = 0
        for p in request.data:
            ts = TimeSeries.from_proto(p)
            res = self.db.timeseries.insert_timeseries(ts)
            res_counter += int(res)
        return PutResult(code=200, counter=res_counter, message="success")

    def lastValues(self, request, context):
        # request: LastValuesRequest
        # return: FloatTimeSeriesList

        if not request.key or not request.metrics:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return FloatTimeSeriesList()

        if request.max_ts:
            try:
                max_dt = pendulum.parse(request.max_ts)
            except ParserError:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("use iso-timestamp for max_ts")
                return FloatTimeSeriesList()
            max_ts = max_dt.int_timestamp
        else:
            max_ts = None

        if request.count:
            count = int(request.count)
        else:
            count = 1

        if request.max_days:
            max_days = int(request.max_days)
        else:
            max_days = 365

        ts_list = self.db.timeseries.get_last_values(request.key, request.metrics, count=count, max_days=max_days, max_ts=max_ts)
        l = FloatTimeSeriesList()
        l.data.extend([r.to_proto() for r in ts_list])
        return l

    def delete(self, request, context):
        # request: MultiTimeSeriesRequest
        # return: DeleteResult

        if not request.key or not request.metrics or not request.from_datetime or not request.to_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return DeleteResult()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for from_datetime and to_datetime")
            return DeleteResult()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        res = self.db.timeseries.delete_timeseries(request.key, request.metrics, from_ts, to_ts)
        return DeleteResult(code=200, counter=int(res), message="success")


class ActivityServicer(ActivityServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, db_instance):
        self.db = db_instance

    def getTotal(self, request, context):
        # request: TotalActivityRequest
        # return: ActivityResponse
        raise NotImplementedError()

    def getDay(self, request, context):
        # request: ActivityDayRequest
        # return: ActivityResponse
        raise NotImplementedError()

    def getReader(self, request, context):
        # request: ReaderActivityRequest
        # return: ActivityResponse
        raise NotImplementedError()

    def increment(self, request, context):
        # request: IncrementActivityRequest
        # return: PutResult
        raise NotImplementedError()


class MetaDataServicer(MetaDataServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, db_instance):
        self.db = db_instance

    def get(self, request, context):
        # request: MetaDataRequest
        # return: MetaDataResponse
        raise NotImplementedError()

    def put(self, request, context):
        # request: MetaDataPost
        # return: PutResult
        raise NotImplementedError()


class EventsServicer(EventsServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, db_instance):
        self.db = db_instance

    def get(self, request, context):
        # request: EventsRequest
        # return: EventSeries

        if not request.key or not request.name or not request.from_datetime or not request.to_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return EventSeries()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for from_datetime and to_datetime")
            return EventSeries()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        ts = self.db.events.get_events(request.key, request.name, from_ts, to_ts)
        return ts.to_proto()

    def lastEvents(self, request, context):
        # request: LastEventsRequest
        # return: EventSeries

        if not request.key or not request.name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return EventSeries()

        if request.max_ts:
            try:
                max_dt = pendulum.parse(request.max_ts)
            except ParserError:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("use iso-timestamp for max_ts")
                return EventSeries()
            max_ts = max_dt.int_timestamp
        else:
            max_ts = None

        if request.count:
            count = int(request.count)
        else:
            count = 1

        if request.max_days:
            max_days = int(request.max_days)
        else:
            max_days = 365

        ts = self.db.events.get_last_events(request.key, request.name, count=count, max_days=max_days, max_ts=max_ts)
        return ts.to_proto()

    def put(self, request, context):
        # request: EventSeries
        # return: PutResult

        if (not request.key or not request.name or not request.timestamps
                or not request.values or not request.timestamp_offsets):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return PutResult()

        assert 2 <= len(request.name) <= 64
        assert 3 <= len(request.key) <= 32
        assert len(request.timestamps) > 0
        assert len(request.values) == len(request.timestamps) == len(request.timestamp_offsets)

        ts = EventList.from_proto(request)
        res = self.db.events.insert_events(ts)
        return PutResult(code=200, counter=int(res), message="success")

    def delete(self, request, context):
        # request: EventsRequest
        # return: DeleteResult

        if not request.key or not request.name or not request.from_datetime or not request.to_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return DeleteResult()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for from_datetime and to_datetime")
            return DeleteResult()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        res = self.db.events.delete_event_days(request.key, request.name, from_ts, to_ts)
        return DeleteResult(code=200, counter=int(res), message="success")
