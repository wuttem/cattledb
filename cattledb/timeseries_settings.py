#!/usr/bin/python
# coding: utf8

from enum import Enum
from collections import MutableSequence
from collections import namedtuple

MetricDefinition = namedtuple('MetricDefinition', ['name', 'id', 'type', 'delete_possible', "update_possible", "resolution"])


class MetricType(Enum):
    FLOAT = 1
    DICT = 2


class Resolution(Enum):
    SECOND = 1
    MINUTE = 2
    HOUR = 3


AVAILABLE_METRICS = [
    MetricDefinition("test", "test", MetricType.FLOAT, True, True, Resolution.SECOND),

    # Raw Metrics
    MetricDefinition("rawph", "rph", MetricType.FLOAT, False, False, Resolution.MINUTE),
    MetricDefinition("adcph", "aph", MetricType.FLOAT, False, False, Resolution.MINUTE),
    MetricDefinition("rawtemp", "rtp", MetricType.FLOAT, False, False, Resolution.MINUTE),
    MetricDefinition("adctemp", "atp", MetricType.FLOAT, False, False, Resolution.MINUTE),
    MetricDefinition("rawact", "rac", MetricType.FLOAT, False, False, Resolution.MINUTE),
    MetricDefinition("rawhum", "rhu", MetricType.FLOAT, False, False, Resolution.MINUTE),

    # Stage 1
    MetricDefinition("ph", "ph", MetricType.FLOAT, True, True, Resolution.MINUTE),
    MetricDefinition("temp", "tmp", MetricType.FLOAT, True, True, Resolution.MINUTE),
    MetricDefinition("act", "act", MetricType.FLOAT, True, True, Resolution.MINUTE),
    MetricDefinition("hum", "hum", MetricType.FLOAT, True, True, Resolution.MINUTE),
    MetricDefinition("act_index", "aci", MetricType.FLOAT, True, True, Resolution.MINUTE),
    MetricDefinition("rawphuncorrected", "uph", MetricType.FLOAT, True, True, Resolution.MINUTE)
]

METRIC_NAME_LOOKUP = {
    m.name: m for m in AVAILABLE_METRICS
}

METRIC_ID_LOOKUP = {
    m.id: m for m in AVAILABLE_METRICS
}

METRIC_NAMES = [m.name for m in AVAILABLE_METRICS]

METRIC_IDS = [m.id for m in AVAILABLE_METRICS]