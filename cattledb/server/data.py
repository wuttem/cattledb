#!/usr/bin/python
# coding: utf8

from sanic.response import json
from sanic import Blueprint
import asyncio

# import aiohttp


data_bp = Blueprint('data', url_prefix='/data')


@data_bp.route('/put', methods=['POST'])
async def put_data(request):
    device_id = request.json["device_id"]
    metric = request.json["metric"]
    data = request.json["data"]
    print(request.app.db.getTest())
    #request.app.db.set_loop(asyncio.get_event_loop())
    res = await request.app.db.put(device_id, metric, data)
    return json({"status": "ok"})

@data_bp.route('/sleep', methods=['POST'])
async def sleep_test(request):
    await asyncio.sleep(1)
    data = {'status': 'sleep'}
    return json(data)

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
