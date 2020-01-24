#!/usr/bin/python
# coding: utf8

from .bigtable import BigtableEngine


def engine_factory(engine_name, read_only, table_prefix, admin=False, engine_options=None):
    if engine_options is None:
        engine_options = {}

    if engine_name == "bigtable":
        return BigtableEngine(engine_options=engine_options, read_only=read_only, table_prefix=table_prefix, admin=admin)
    raise ValueError("invalid storage engine")
