#!/usr/bin/python
# coding: utf-8

import logging
import logging.config
import os


def setup_logging(config):
    if hasattr(config, "LOGGING_CONFIG"):
        logging.config.dictConfig(config.LOGGING_CONFIG)
    else:
        logging.basicConfig(level=logging.INFO)

# @app.listener('before_server_start')
# def init_db(app, loop):
#     from ..directclient import AsyncCDBClient
#     config = app.cdb_config
#     # Setup DB
#     engine = config.ENGINE
#     engine_options = config.ENGINE_OPTIONS
#     read_only = config.READ_ONLY
#     pool_size = config.POOL_SIZE
#     admin = config.ADMIN
#     table_prefix = config.TABLE_PREFIX
#     app.db = AsyncCDBClient(engine=engine, engine_options=engine_options, table_prefix=table_prefix,
#                             pool_size=pool_size, read_only=read_only, admin=admin)
#     app.db.service_init()
#     logging.getLogger().warning("DB Setup finished")


def _create_app(config):
    from flask import Flask
    app = Flask("cattledb")

    # Setting Hostname
    import socket
    host_name = str(socket.gethostname())
    logging.getLogger().warning("Creating App on %s", host_name)

    # setup
    from .ext import FlaskCDB
    db_ext = FlaskCDB(engine=config.ENGINE, engine_options=config.ENGINE_OPTIONS,
                      read_only=config.READ_ONLY, admin=config.ADMIN,
                      table_prefix=config.TABLE_PREFIX, app=app)
    # warmup
    db_ext.warmup(app)
    app.cdb = db_ext

    from .services import bp
    app.register_blueprint(bp)

    return app


def create_app_by_config(config_name=None):
    if config_name is None:
        config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
    config_name = config_name.strip()

    from ..settings import available_configs

    selected_config = available_configs[config_name]
    logging.getLogger().warning("Using Config: {}".format(selected_config))
    setup_logging(selected_config)

    return _create_app(selected_config)
