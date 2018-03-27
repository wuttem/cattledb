#!/usr/bin/python
# coding: utf8

from sanic.response import json, HTTPResponse, raw
from sanic import Blueprint
from sanic.exceptions import InvalidUsage
import asyncio
import pendulum
from pendulum.parsing.exceptions import ParserError

# import aiohttp


data_bp = Blueprint('data', url_prefix='/data')


@data_bp.route('/put_proto', methods=['POST'])
async def put_proto_data(request):
    res = await request.app.db.put_proto(request.body)
    return json({"status": "ok"})


@data_bp.route('/get_proto', methods=['GET'])
async def get_proto_data(request):
    device_id = request.args.get("device_id", None)
    if device_id is None:
        raise InvalidUsage("device_id missing")
    metric = request.args.get("metric", None)
    metrics = request.args.getlist("metrics", None)
    if metric is None and metrics is None:
        raise InvalidUsage("metric or metrics missing")
    if metrics is None:
        metrics = [metric]

    try:
        from_dt = pendulum.parse(request.args.get("from_dt"))
        to_dt = pendulum.parse(request.args.get("to_dt"))
    except ParserError:
        raise InvalidUsage("use iso-timestamp for from_dt and to_dt")
    from_ts = from_dt.int_timestamp
    to_ts = to_dt.int_timestamp
    res = await request.app.db.get_proto(device_id, metrics, from_ts, to_ts)
    return raw(res)


@data_bp.route('/put_json', methods=['POST'])
async def put_json_data(request):
    device_id = request.json["device_id"]
    metric = request.json["metric"]
    data = request.json["data"]
    data_ts = map(lambda i: (pendulum.parse(i[0]), i[1]), data)
    res = await request.app.db.put(device_id, metric, data_ts)
    return json({"status": "ok"})


@data_bp.route('/get_json', methods=['GET'])
async def get_json_data(request):
    device_id = request.args.get("device_id", None)
    if device_id is None:
        raise InvalidUsage("device_id missing")
    metric = request.args.get("metric", None)
    metrics = request.args.getlist("metrics", None)
    if metric is None and metrics is None:
        raise InvalidUsage("metric or metrics missing")
    if metrics is None:
        metrics = [metric]

    try:
        from_dt = pendulum.parse(request.args.get("from_dt"))
        to_dt = pendulum.parse(request.args.get("to_dt"))
    except ParserError:
        raise InvalidUsage("use iso-timestamp for from_dt and to_dt")
    from_ts = from_dt.int_timestamp
    to_ts = to_dt.int_timestamp
    res = await request.app.db.get(device_id, metrics, from_ts, to_ts)
    return json([{"data": r.to_serializable(), "metric": r.key, "device_id": device_id} for r in res])


@data_bp.route('/get_last', methods=['GET'])
async def get_last_data(request):
    count = request.args.get("count", 1)
    device_id = request.args.get("device_id", None)
    if device_id is None:
        raise InvalidUsage("device_id missing")
    metric = request.args.get("metric", None)
    if metric is None:
        raise InvalidUsage("metric missing")

    res = await request.app.db.get_last(device_id, [metric], count, max_days=2)
    return json([{"data": r.to_serializable(), "metric": r.key, "device_id": device_id} for r in res])


# async def fetch(session, url):
#     """
#     Use session object to perform 'get' request on url
#     """
#     async with session.get(url) as result:
#         return await result.json()


# @data_bp.route('/testasync')
# async def handle_request(request):
#     url = "https://api.github.com/repos/channelcat/sanic"
    
#     async with aiohttp.ClientSession() as session:
#         result = await fetch(session, url)
#         return response.json(result)
