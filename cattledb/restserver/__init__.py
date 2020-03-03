#!/usr/bin/python
# coding: utf-8

import logging


def setup_logging(config):
    if hasattr(config, "LOGGING_CONFIG"):
        logging.config.dictConfig(config.LOGGING_CONFIG)
    else:
        logging.basicConfig(level=logging.INFO)


def create_server(config):
    pass