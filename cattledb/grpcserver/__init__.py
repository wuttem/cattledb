#!/usr/bin/python
# coding: utf8

import logging
import logging.config
import os
import grpc
from concurrent import futures

from .cdb_pb2_grpc import add_TimeSeriesServicer_to_server, add_EventsServicer_to_server, add_MetaDataServicer_to_server, add_ActivityServicer_to_server


def setup_logging(config):
    if hasattr(config, "LOGGING_CONFIG"):
        logging.config.dictConfig(config.LOGGING_CONFIG)
    else:
        logging.basicConfig(level=logging.INFO)


def create_server(config):
    from ..storage.connection import Connection
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.POOL_SIZE))

    # Setup DB
    project_id = config.GCP_PROJECT_ID
    instance_id = config.GCP_INSTANCE_ID
    credentials = config.GCP_CREDENTIALS
    read_only = config.READ_ONLY
    pool_size = config.POOL_SIZE
    table_prefix = config.TABLE_PREFIX
    metrics = config.METRICS
    if config.STAGING:
         read_only = True
    db_connection = Connection(project_id=project_id, instance_id=instance_id, read_only=read_only,
                               pool_size=pool_size, table_prefix=table_prefix, credentials=credentials,
                               metric_definition=metrics)
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


def create_server_by_config(config_name=None):
    if config_name is None:
        config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
    config_name = config_name.strip()

    from ..settings import available_configs

    selected_config = available_configs[config_name]
    logging.getLogger().warning("Using Config: {}".format(selected_config))
    setup_logging(selected_config)

    # Setting Hostname
    import socket
    host_name = str(socket.gethostname())
    logging.getLogger().warning("Creating gRPC Service on %s(%s)", host_name, config_name)

    return create_server(selected_config)
