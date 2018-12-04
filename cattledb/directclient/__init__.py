#!/usr/bin/python
# coding: utf8

import logging
import logging.config


def setup_logging(config):
    if hasattr(config, "LOGGING_CONFIG"):
        logging.config.dictConfig(config.LOGGING_CONFIG)
    else:
        logging.basicConfig(level=logging.INFO)


def create_client(config, setup_logging=True):
    from ..storage.connection import Connection

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

    if setup_logging:
        setup_logging(config)

    return Connection(project_id=project_id, instance_id=instance_id, read_only=read_only,
                      pool_size=pool_size, table_prefix=table_prefix, credentials=credentials,
                      metric_definition=metrics)