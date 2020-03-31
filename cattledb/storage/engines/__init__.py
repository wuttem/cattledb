#!/usr/bin/python
# coding: utf-8

from .bigtable import BigtableEngine
from .localsql import SQLiteEngine


def get_engine_capabilities(engine_name):
    if engine_name == "bigtable":
        return {"threading": True}
    if engine_name == "localsql":
        return {"threading": False}
    raise ValueError("invalid storage engine")


def engine_factory(engine_name, read_only, table_prefix, admin=False, engine_options=None):
    if engine_options is None:
        engine_options = {}

    if engine_name == "bigtable":
        return BigtableEngine(engine_options=engine_options, read_only=read_only, table_prefix=table_prefix, admin=admin)
    if engine_name == "localsql":
        return SQLiteEngine(engine_options=engine_options, read_only=read_only, table_prefix=table_prefix, admin=admin)
    raise ValueError("invalid storage engine")
