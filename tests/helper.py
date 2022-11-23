#!/usr/bin/python
# coding: utf-8

from cattledb.storage.connection import Connection
from cattledb.settings import testing_bigtable as test_config_bigtable
from cattledb.settings import testing_dynamo as test_config_dynamo
from cattledb.settings import testing_local as test_config_local

def get_unit_test_config(engine="bigtable"):
    if engine == "bigtable":
        return test_config_bigtable
    elif engine == "dynamo":
        return test_config_dynamo
    elif engine == "local":
        return test_config_local
    raise RuntimeError


def get_test_metrics():
    config = get_unit_test_config()
    return config.METRICS


def get_test_events():
    config = get_unit_test_config()
    return config.EVENTS


def get_test_connection():
    return Connection.from_config(get_unit_test_config())
