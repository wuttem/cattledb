import grpc
import pendulum
from pendulum.parsing.exceptions import ParserError
from .cdb_pb2 import FloatTimeSeries, FloatTimeSeriesList, PutResult, DeleteResult, EventSeries, MetaDataResponse, ActivityResponse, DeviceActivityResponse
from .cdb_pb2_grpc import TimeSeriesServicer, ActivityServicer, MetaDataServicer, EventsServicer
from ..storage.models import TimeSeries, EventList, SerializableNamespaceDict, MetaDataItem


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

        ts_list = self.db.timeseries.get_last_values(request.key, request.metrics)
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
        if not request.day_datetime:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return ActivityResponse()

        try:
            dt = pendulum.parse(request.day_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for day_datetime")
            return ActivityResponse()

        day_ts = dt.int_timestamp

        res = self.db.activity.get_total_activity_for_day(day_ts)
        return ActivityResponse(activities=[x.to_proto() for x in res])

    def getDay(self, request, context):
        # request: ActivityDayRequest
        # return: ActivityResponse
        if not request.day_datetime or not request.parent_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return ActivityResponse()

        try:
            dt = pendulum.parse(request.day_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for day_datetime")
            return ActivityResponse()

        day_ts = dt.int_timestamp

        res = self.db.activity.get_activity_for_day(request.parent_id, day_ts)
        return ActivityResponse(activities=[x.to_proto() for x in res])

    def getReader(self, request, context):
        # request: ReaderActivityRequest
        # return: DeviceActivityResponse
        if not request.from_datetime or not request.to_datetime or not request.reader_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return DeviceActivityResponse()

        try:
            from_dt = pendulum.parse(request.from_datetime)
            to_dt = pendulum.parse(request.to_datetime)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for day_datetime")
            return DeviceActivityResponse()

        from_ts = from_dt.int_timestamp
        to_ts = to_dt.int_timestamp

        res = self.db.activity.get_activity_for_reader(request.reader_id, from_ts, to_ts)
        return DeviceActivityResponse(activities=[x.to_proto() for x in res])

    def increment(self, request, context):
        # request: IncrementActivityRequest
        # return: PutResult

        if not request.reader_id or not request.device_id or not request.timestamp:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return PutResult()

        try:
            dt = pendulum.parse(request.timestamp)
        except ParserError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("use iso-timestamp for timestamp")
            return PutResult()

        ts = dt.int_timestamp

        if request.value:
            value = int(request.value)
        else:
            value = 1

        if request.parent_ids:
            parents = list(request.parent_ids)
        else:
            parents = None

        res = self.db.activity.incr_activity(request.reader_id, request.device_id,
                                             timestamp=ts, parent_ids=parents, value=value)
        return PutResult(code=200, counter=len(res), message="success")


class MetaDataServicer(MetaDataServicer):
    """Provides methods that implement functionality of route guide server."""
    def __init__(self, db_instance):
        self.db = db_instance

    def get(self, request, context):
        # request: MetaDataRequest
        # return: MetaDataResponse
        if not request.object_name or not request.object_key:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return MetaDataResponse()

        if not request.namespaces:
            namespaces = None
        else:
            namespaces = list(request.namespaces)

        if not request.internal:
            internal = False
        else:
            internal = list(request.internal)

        res = MetaDataResponse(object_name=request.object_name, object_key=request.object_key)
        md = self.db.metadata.get_metadata(request.object_name, request.object_key, keys=namespaces, internal=internal)

        proto_dicts = []
        if md is not None:
            for i in md:
                proto_dicts.append(SerializableNamespaceDict(i.key, i.data).to_proto())
        res.data.extend(proto_dicts)

        return res

    def put(self, request, context):
        # request: MetaDataPost
        # return: PutResult
        if not request.object_name or not request.object_key or len(request.data) < 1:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Invalid Request')
            return PutResult()

        if not request.internal:
            internal = False
        else:
            internal = list(request.internal)

        metas = []
        for item in request.data:
            d = SerializableNamespaceDict.from_proto(item)
            metas.append(MetaDataItem(request.object_name, request.object_key, d.namespace, d.to_dict()))

        res = self.db.metadata.put_metadata_items(metas, internal=internal)
        return PutResult(code=200, counter=int(res), message="success")


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

        ts = self.db.events.get_last_events(request.key, request.name)
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
