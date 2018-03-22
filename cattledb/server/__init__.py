#!/usr/bin/python
# coding: utf8

import logging
import os
from sanic import Sanic

from ..settings import available_configs
from ..storage import AsyncDB


def setup_logging(config):
    # if "CLOUD_LOGGING" in config and config["CLOUD_LOGGING"]:
    #     logger = logging.getLogger()
    #     logger.setLevel(logging.INFO)
    #     handler = logging.StreamHandler()
    #     handler.setFormatter(CloudLoggingFormatter(service_name="anthilldata"))
    #     logger.addHandler(handler)
    if "LOGGING_CONFIG" in config:
        logging.config.dictConfig(config["LOGGING_CONFIG"])
    else:
        logging.basicConfig(level=logging.INFO)


async def setup_db(app, loop):
    project_id = app.config.GCP_PROJECT_ID
    instance_id = app.config.GCP_INSTANCE_ID
    credentials = app.config.GCP_CREDENTIALS
    read_only = app.config.READ_ONLY
    pool_size = app.config.POOL_SIZE
    table_prefix = app.config.TABLE_PREFIX
    if app.config.STAGING:
         read_only=True
    app.db = AsyncDB(project_id=project_id, instance_id=instance_id, loop=loop,
                     read_only=read_only, pool_size=pool_size, table_prefix=table_prefix,
                     credentials=credentials)


async def close_db(app, loop):
    app.db = None


def create_app(settings_override=None,
               config_name=None):
    tempdir = os.path.dirname(os.path.realpath(__file__))
    tempdir = os.path.join(tempdir, "..")
    app = Sanic(load_env='CATTLEDB_')

    if config_name is None:
        config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
    config_name = config_name.strip()

    selected_config = available_configs[config_name]
    logging.getLogger().warning("Using Config: {}".format(selected_config))
    app.config.from_object(selected_config)
    #app.config.from_pyfile('settings.cfg', silent=True)
    #app.config.from_object(settings_override)

    setup_logging(app.config)

    # Init Db
    app.listener('before_server_start')(setup_db)
    app.listener('after_server_stop')(close_db)

    # Setting Hostname
    import socket
    app.host_name = str(socket.gethostname())
    logging.getLogger().warning("Creating App(%s)", config_name)
    #app.before_request(my_before_request)
    #app.after_request(my_after_request)

    # Register API
    from .base import base_bp
    app.blueprint(base_bp)
    from .data import data_bp
    app.blueprint(data_bp)

    return app