#!/usr/bin/python
# coding: utf-8

import pathlib
import logging
import csv


from ..core.models import FastFloatTimeseries, FastDictTimeseries

logger = logging.getLogger(__name__)


class LocalFileStore(object):
    def __init__(self, data_dir):
        self.data_dir = pathlib.Path(data_dir).absolute()
        if not self.data_dir.exists():
            logger.warning("creating data directory: {}".format(self.data_dir))
            self.data_dir.mkdir(parents=True, exist_ok=True)
        assert self.data_dir.is_dir()

    def get_file(self, filename, subfolder=None):
        if subfolder is not None:
            sub_dir = self.data_dir / subfolder
            if not sub_dir.exists():
                sub_dir.mkdir(parents=False, exist_ok=True)
            filepath = sub_dir / filename
        else:
            filepath = self.data_dir / filename
        return filepath

    def get_file_for_key(self, key, subfolder=None, missing=True):
        filepath = self.get_file("{}.csv".format(key), subfolder=subfolder)
        if missing:
            filepath.touch(exist_ok=True)
        if not filepath.exists() or filepath.is_dir():
            raise FileNotFoundError("File {} not found".format(filepath))
        return filepath

    def get_timeseries(self, key, subfolder=None):
        file_name = self.get_file_for_key(key, subfolder=None)
        values = []
        metrics = set()
        with open(file_name) as fp:
            r = csv.DictReader(fp, quoting=csv.QUOTE_NONNUMERIC)
            for x in r:
                ts = x.pop("ts")
                metrics.update(x.keys())
                values.append((ts, x))
        ts = FastDictTimeseries(key=key, metric="multi", values=values)
        ts.set_columns(list(sorted(metrics)))
        return ts

    def store_timeseries(self, series, subfolder=None):
        assert isinstance(series, FastDictTimeseries)
        file_name = self.get_file_for_key(series.key, subfolder=None)
        with open(file_name, "w") as fp:
            series.to_csv(fp)

    def insert_measurements(self, key, ts, value_dict):
        data = self.get_timeseries(key)
        old_metrics = data.columns or []
        metrics = set(old_metrics)
        
        checked_dict = {}
        for k, v in value_dict.items():
            checked_dict[k] = float(v)

        metrics.update(checked_dict.keys())
        data.insert_point(ts, checked_dict)
        data.set_columns(list(sorted(metrics)))

        self.store_timeseries(data)
