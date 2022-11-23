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


class FColumn():
    def __init__(self, cf, cn):
        self.cf = cf
        self.cn = cn

    @classmethod
    def from_string(cls, s):
        assert ":" in s
        fam, col = s.split(":", 1)
        return cls(fam, col)

    def __str__(self):
        return "{}:{}".format(self.cf, self.cn)

    def parts(self):
        return self.cf, self.cn


class EventSeriesType(Enum):
    DAILY = 1
    MONTHLY = 2
