#!/usr/bin/python
# coding: utf-8

import logging
import logging.config
import os


# def init_s_db(app, loop):
#     from ..directclient import AsyncCDBClient
#     config = app.cdb_config
#     # Setup DB
#     engine = config.ENGINE
#     engine_options = config.ENGINE_OPTIONS
#     read_only = config.READ_ONLY
#     pool_size = config.POOL_SIZE
#     admin = config.ADMIN
#     table_prefix = config.TABLE_PREFIX
#     app.cdb = AsyncCDBClient(engine=engine, engine_options=engine_options, table_prefix=table_prefix,
#                             pool_size=pool_size, read_only=read_only, admin=admin)
#     app.cdb.service_init()
#     logging.getLogger().warning("DB Setup finished")


def _create_app(config):
    from ..core.helper import setup_logging

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


def create_app_by_configfile(configfile=None):
    from ..core.helper import import_config_file
    from ..settings import default as _default_config

    if configfile:
        _imported = import_config_file(configfile)
        click.echo("Using Config: {}".format(configfile))
        config = _imported
    else:
        config = _default_config
        click.echo("Using Default Config")

    return _create_app(config)


# def create_sanic_app_by_config(config_name=None):
#     if config_name is None:
#         config_name = os.getenv('CATTLEDB_CONFIGURATION', 'default')
#     config_name = config_name.strip()

#     from ..settings import available_configs

#     selected_config = available_configs[config_name]
#     logging.getLogger().warning("Using Config: {}".format(selected_config))
#     setup_logging(selected_config)

#     from sanic import Sanic
#     app = Sanic()
#     from .s_services import bp
#     app.blueprint(bp)
#     app.cdb_config = selected_config
#     app.listener('before_server_start')(init_s_db)

#     return app
