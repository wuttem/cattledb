#!/usr/bin/python
# coding: utf8

from sanic.response import json
from sanic import Blueprint
import asyncio

from .. import __version__ as version

base_bp = Blueprint('service', url_prefix='/service')

@base_bp.route('/')
async def bp_root(request):
    return json({'status': 'ok', "version": version})

@base_bp.route('/sleep')
async def sleep_test(request):
    await asyncio.sleep(1)
    data = {'status': 'sleep'}
    return json(data)