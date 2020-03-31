#!/usr/bin/python
# coding: utf-8

from abc import ABCMeta, abstractmethod


class StorageEngine(metaclass=ABCMeta):
    def __init__(self, engine_options, read_only=False, table_prefix="mycdb", admin=False):
        self.read_only = read_only
        self.table_prefix = table_prefix
        self.admin = admin
        self.db_connection = None
        self.admin_connection = None
        self.engine_options = engine_options
        self.setup_engine_options(self.engine_options)
        self.connect()

    @abstractmethod
    def setup_engine_options(self, engine_options):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def setup_table(self, table_name, silent=False):
        pass

    @abstractmethod
    def setup_column_family(self, table_name, column_family, silent=False):
        pass

    @abstractmethod
    def get_table(self, table_name):
        pass

    @abstractmethod
    def get_admin_table(self, table_name):
        pass

    def get_full_table_name(self, table_name):
        return "{}_{}".format(self.table_prefix, table_name)


class StorageTable(metaclass=ABCMeta):
    @abstractmethod
    def write_cell(self, row_id, column, value):
        pass

    @abstractmethod
    def read_row(self, row_id, column_families=None):
        pass

    @abstractmethod
    def delete_row(self, row_id, column_families=None):
        pass

    @abstractmethod
    def upsert_row(self, row_id, values):
        pass

    @abstractmethod
    def upsert_rows(self, row_upserts):
        pass

    @abstractmethod
    def row_generator(self, row_keys=None, start_key=None, end_key=None,
                      column_families=None, check_prefix=None):
        pass

    @abstractmethod
    def get_first_row(self, start_key, column_families=None, end_key=None):
        pass

    @abstractmethod
    def increment_counter(self, row_id, column, value):
        pass

    def read_rows(self, row_keys=None, start_key=None, end_key=None,
                  column_families=None, check_prefix=None):
        generator = self.row_generator(row_keys=row_keys, start_key=start_key, end_key=end_key,
                                       column_families=column_families, check_prefix=check_prefix)
        return [(rk, data) for rk, data in generator]

    @abstractmethod
    def get_column_families(self):
        pass
