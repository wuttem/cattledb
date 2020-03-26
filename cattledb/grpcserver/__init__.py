#!/usr/bin/python
# coding: utf-8
from builtins import str

import os
import logging
import grpc
from concurrent import futures

from .cdb_pb2_grpc import add_TimeSeriesServicer_to_server, add_EventsServicer_to_server, add_MetaDataServicer_to_server, add_ActivityServicer_to_server


logger = logging.getLogger(__name__)


def _create_server(config):
    from ..core.helper import setup_logging
    setup_logging(config)

    from ..storage.connection import Connection
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.POOL_SIZE))

    # Setup DB
    db_connection = Connection.from_config(config)
    server.db = db_connection

    from .services import TimeSeriesServicer
    ts_store = TimeSeriesServicer(db_connection)
    add_TimeSeriesServicer_to_server(ts_store, server)
    from .services import EventsServicer
    ev_store = EventsServicer(db_connection)
    add_EventsServicer_to_server(ev_store, server)
    from .services import MetaDataServicer
    meta_store = MetaDataServicer(db_connection)
    add_MetaDataServicer_to_server(meta_store, server)
    from .services import ActivityServicer
    act_store = ActivityServicer(db_connection)
    add_ActivityServicer_to_server(act_store, server)

    return server


def create_server_by_configfile(configfile=None):
    from ..core.helper import import_config_file
    from ..settings import default as _default_config

    if configfile:
        _imported = import_config_file(configfile)
        logger.warning("Using Config: {}".format(configfile))
        config = _imported
    else:
        config = _default_config
        logger.warning("Using Default Config")

    return _create_server(config)


# def create_server_by_config(config_name=None):
#     if config_name is None:
#         config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
#     config_name = config_name.strip()

#     from ..settings import available_configs

#     selected_config = available_configs[config_name]
#     logging.getLogger().warning("Using Config: {}".format(selected_config))
#     setup_logging(selected_config)

#     # Setting Hostname
#     import socket
#     host_name = str(socket.gethostname())
#     logging.getLogger().warning("Creating gRPC Service on %s(%s)", host_name, config_name)

#     return create_server(selected_config)
