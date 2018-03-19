#!/usr/bin/python
# coding: utf8

import logging
import os
from sanic import Sanic


from .settings import available_configs


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

    # Setting Hostname
    import socket
    app.host_name = str(socket.gethostname())
    logging.getLogger().warning("Creating App(%s)", config_name)
    #app.before_request(my_before_request)
    #app.after_request(my_after_request)

    # Main API
    from .api import register
    register(app)

    return app