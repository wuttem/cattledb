#!/usr/bin/python
# coding: utf8

import logging
import os
from enum import Enum
from collections import namedtuple

MetricDefinition = namedtuple('MetricDefinition', ['name', 'id', 'type', 'delete_possible', "update_possible", "resolution"])
EventDefinition = namedtuple('EventDefinition', ['name', "type", "resolution"])


class MetricType(Enum):
    FLOATSERIES = 1
    DICTSERIES = 2


class EventSeriesType(Enum):
    DAILY = 1
    MONTHLY = 2


class Resolution(Enum):
    SECOND = 1
    MINUTE = 2
    HOUR = 3


AVAILABLE_METRICS = [
    MetricDefinition("test", "test", MetricType.FLOATSERIES, True, True, Resolution.SECOND),

    # Raw Metrics
    MetricDefinition("rawph", "rph", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),
    MetricDefinition("adcph", "aph", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),
    MetricDefinition("rawtemp", "rtp", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),
    MetricDefinition("adctemp", "atp", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),
    MetricDefinition("rawact", "rac", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),
    MetricDefinition("rawhum", "rhu", MetricType.FLOATSERIES, False, False, Resolution.MINUTE),

    # Stage 1
    MetricDefinition("ph", "ph", MetricType.FLOATSERIES, True, True, Resolution.MINUTE),
    MetricDefinition("temp", "tmp", MetricType.FLOATSERIES, True, True, Resolution.MINUTE),
    MetricDefinition("act", "act", MetricType.FLOATSERIES, True, True, Resolution.MINUTE),
    MetricDefinition("hum", "hum", MetricType.FLOATSERIES, True, True, Resolution.MINUTE),
    MetricDefinition("act_index", "aci", MetricType.FLOATSERIES, True, True, Resolution.MINUTE),
    MetricDefinition("rawphuncorrected", "uph", MetricType.FLOATSERIES, True, True, Resolution.MINUTE)
]

EVENT_TYPES = [
    EventDefinition("test_daily", EventSeriesType.DAILY, Resolution.SECOND),
    EventDefinition("test_monthly", EventSeriesType.MONTHLY, Resolution.MINUTE),
    EventDefinition("test_monthly_*", EventSeriesType.MONTHLY, Resolution.MINUTE)
]


class BaseConfig(object):
    TESTING = True
    DEBUG = True
    STAGING = False

    GCP_PROJECT_ID = 'test-system'
    GCP_INSTANCE_ID = 'test'
    GCP_CREDENTIALS = None
    READ_ONLY = False
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
