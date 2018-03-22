#!/usr/bin/python
# coding: utf8

import logging
import os


class BaseConfig(object):
    TESTING = True
    DEBUG = True
    STAGING = False

    GCP_PROJECT_ID = 'test-system'
    GCP_INSTANCE_ID = 'test'
    GCP_CREDENTIALS = None
    READ_ONLY = False
    POOL_SIZE = 10
    TABLE_PREFIX = "cdb"

    CLOUD_LOGGING = False

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "stream": "ext://sys.stdout"
            }
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"]
        }
    }


class DevelopmentConfig(BaseConfig):
    pass


class UnitTestConfig(BaseConfig):
    pass


class LiveConfig(BaseConfig):
    pass


class StagingConfig(BaseConfig):
    STAGING = True


available_configs = {
    "development": DevelopmentConfig,
    "testing": UnitTestConfig,
    "live": LiveConfig,
    "staging": StagingConfig,
    "default": DevelopmentConfig
}


# def includeEnvironmentVars(config):
#     for key, value in os.environ.iteritems():
#         if key.upper().startswith("ANTHILLCONTROL_") and key.upper() != "ANTHILLCONTROL_CONFIGURATION":
#             new_key = key.upper().replace("ANTHILLCONTROL_", "")
#             if value.upper() == "TRUE":
#                 new_value = True
#             elif value.upper() == "FALSE":
#                 new_value = False
#             else:
#                 new_value = value
#             setattr(config, new_key, new_value)
