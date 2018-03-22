#!/usr/bin/python
# coding: utf8

from sanic.response import json
from sanic import Blueprint
import asyncio
import pendulum

# import aiohttp


data_bp = Blueprint('data', url_prefix='/data')


@data_bp.route('/put', methods=['POST'])
async def put_data(request):
    device_id = request.json["device_id"]
    metric = request.json["metric"]
    data = request.json["data"]
    data_ts = map(lambda i: (pendulum.parse(i[0]), i[1]), data)
    res = await request.app.db.put(device_id, metric, data_ts)
    return json({"status": "ok"})


@data_bp.route('/get', methods=['GET'])
async def get_data(request):
    device_id = request.args.get("device_id")
    metric = request.args.get("metric")
    from_dt = pendulum.parse(request.args.get("from_dt"))
    to_dt = pendulum.parse(request.args.get("to_dt"))
    from_ts = from_dt.int_timestamp
    to_ts = to_dt.int_timestamp
    res = await request.app.db.get(device_id, metric, from_ts, to_ts)
    data = res.to_serializable()
    return json({"data": data})


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
