#!/usr/bin/python
# coding: utf8

from sanic.response import json
from sanic import Blueprint
import asyncio
import aiohttp


base_bp = Blueprint('data', url_prefix='/data')


async def fetch(session, url):
    """
    Use session object to perform 'get' request on url
    """
    async with session.get(url) as result:
        return await result.json()


@app.route('/')
async def handle_request(request):
    url = "https://api.github.com/repos/channelcat/sanic"
    
    async with aiohttp.ClientSession() as session:
        result = await fetch(session, url)
        return response.json(result)