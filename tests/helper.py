#!/usr/bin/python
# coding: utf-8

from cattledb.storage.connection import Connection
from cattledb.settings import testing as test_config


def get_unit_test_config():
    return test_config


def get_test_metrics():
    config = get_unit_test_config()
    return config.METRICS


def get_test_events():
    config = get_unit_test_config()
    return config.EVENTS


def get_test_connection():
    return Connection.from_config(get_unit_test_config())
