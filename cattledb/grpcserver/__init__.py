#!/usr/bin/python
# coding: utf8

import logging
import logging.config
import os
import grpc
from concurrent import futures

from .cdb_pb2_grpc import add_TimeSeriesServicer_to_server
from ..settings import available_configs


def setup_logging(config):
    # if "CLOUD_LOGGING" in config and config["CLOUD_LOGGING"]:
    #     logger = logging.getLogger()
    #     logger.setLevel(logging.INFO)
    #     handler = logging.StreamHandler()
    #     handler.setFormatter(CloudLoggingFormatter(service_name="anthilldata"))
    #     logger.addHandler(handler)
    if hasattr(config, "LOGGING_CONFIG"):
        logging.config.dictConfig(config.LOGGING_CONFIG)
    else:
        logging.basicConfig(level=logging.INFO)


def create_server(settings_override=None,
                  config_name=None):
    tempdir = os.path.dirname(os.path.realpath(__file__))
    tempdir = os.path.join(tempdir, "..")

    if config_name is None:
        config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
    config_name = config_name.strip()

    selected_config = available_configs[config_name]
    logging.getLogger().warning("Using Config: {}".format(selected_config))
    config = selected_config

    # Logging
    setup_logging(config)
    #app.config.from_object(selected_config)
    #app.config.from_pyfile('settings.cfg', silent=True)
    #app.config.from_object(settings_override)

    # Setting Hostname
    import socket
    host_name = str(socket.gethostname())
    logging.getLogger().warning("Creating gRPC Service on %s(%s)", host_name, config_name)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.POOL_SIZE))

    # Setup DB
    project_id = config.GCP_PROJECT_ID
    instance_id = config.GCP_INSTANCE_ID
    credentials = config.GCP_CREDENTIALS
    read_only = config.READ_ONLY
    pool_size = config.POOL_SIZE
    table_prefix = config.TABLE_PREFIX
    if config.STAGING:
         read_only=True
    from .services import TimeSeriesServicer
    db_service = TimeSeriesServicer(project_id, instance_id, read_only=read_only,
                                    pool_size=pool_size, table_prefix=table_prefix,
                                    credentials=credentials)
    add_TimeSeriesServicer_to_server(db_service, server)

    return server