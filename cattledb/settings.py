#!/usr/bin/python
# coding: utf-8

import logging
import os
from enum import Enum
from collections import namedtuple

from .core.models import MetricDefinition, EventDefinition, MetricType, EventSeriesType


AVAILABLE_METRICS = [
    MetricDefinition("test", "test", MetricType.FLOATSERIES, True),

    # Raw Metrics
    MetricDefinition("rawph", "rph", MetricType.FLOATSERIES, False),
    MetricDefinition("adcph", "aph", MetricType.FLOATSERIES, False),
    MetricDefinition("rawtemp", "rtp", MetricType.FLOATSERIES, False),
    MetricDefinition("adctemp", "atp", MetricType.FLOATSERIES, False),
    MetricDefinition("rawact", "rac", MetricType.FLOATSERIES, False),
    MetricDefinition("rawhum", "rhu", MetricType.FLOATSERIES, False),

    # Stage 1
    MetricDefinition("ph", "ph", MetricType.FLOATSERIES, True),
    MetricDefinition("temp", "tmp", MetricType.FLOATSERIES, True),
    MetricDefinition("act", "act", MetricType.FLOATSERIES, True),
    MetricDefinition("hum", "hum", MetricType.FLOATSERIES, True),
    MetricDefinition("act_index", "aci", MetricType.FLOATSERIES, True),
    MetricDefinition("rawphuncorrected", "uph", MetricType.FLOATSERIES, True)
]

EVENT_TYPES = [
    EventDefinition("test_daily", EventSeriesType.DAILY),
    EventDefinition("test_monthly", EventSeriesType.MONTHLY),
    EventDefinition("test_monthly_*", EventSeriesType.MONTHLY)
]


class BaseConfig(object):
    TESTING = True
    DEBUG = True
    STAGING = False

    ENGINE = "bigtable"
    ENGINE_OPTIONS = {
        "credentials": None,
        "project_id": "proj1",
        "instance_id": "inst1",
        "admin": True
    }

    READ_ONLY = False
    ADMIN = True
    POOL_SIZE = 10
    TABLE_PREFIX = "mycdb"

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

    METRICS = AVAILABLE_METRICS
    EVENTS = EVENT_TYPES


class DevelopmentConfig(BaseConfig):
    pass


class UnitTestConfig(BaseConfig):
    pass


class LiveConfig(BaseConfig):
    TESTING = False
    DEBUG = False
    STAGING = False


class StagingConfig(BaseConfig):
    STAGING = True


available_configs = {
    "development": DevelopmentConfig,
    "testing": UnitTestConfig,
    "live": LiveConfig,
    "staging": StagingConfig,
    "default": DevelopmentConfig
}
