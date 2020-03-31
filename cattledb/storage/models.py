#!/usr/bin/python
# coding: utf-8

from enum import Enum

from ..core.models import FastFloatTimeseries
from ..core.models import TimeSeries

from ..core.models import FastDictTimeseries
from ..core.models import EventList

from ..core.models import SerializableDict
from ..core.models import SerializableNamespaceDict
from ..core.models import RowUpsert
from ..core.models import MetaDataItem
from ..core.models import ReaderActivityItem
from ..core.models import DeviceActivityItem

class EventSeriesType(Enum):
    DAILY = 1
    MONTHLY = 2
