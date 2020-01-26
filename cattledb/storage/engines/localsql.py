#!/usr/bin/python
# coding: utf8

import sqlite3
import logging
import os
import json
import base64

from sqlite3 import OperationalError

from .base import StorageEngine, StorageTable

logger = logging.getLogger(__name__)


class SQLiteEngine(StorageEngine):
    def setup_engine_options(self, engine_options):
        self.data_dir = None
        self.in_memory = False

        if "data_dir" not in engine_options:
            raise ValueError("missing data_dir option for sqlite engine")
        else:
            self.data_dir = engine_options["data_dir"]

        if "in_memory" in engine_options:
            self.in_memory = True


    def connect(self):
        if self.db_connection is None:
            f = ":memory:" if self.in_memory else os.path.join(self.data_dir, "cattle.db")
            self.db_connection = sqlite3.connect(f, uri=self.read_only)
        return self.db_connection

    def disconnect(self):
        if self.db_connection is not None:
            self.db_connection.close()

    def setup_table(self, table_name, silent=False):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")

        con = self.connect()
        full_table_name = self.get_full_table_name(table_name)
        _SQL = """CREATE TABLE {}
        (
            k TEXT PRIMARY KEY,
            row_meta TEXT
        )
        """.format(full_table_name)
        try:
            con.execute(_SQL)
        except OperationalError as e:
            if silent:
                logger.warning("CREATE: TABLE {} ALREADY EXISTING".format(full_table_name))
                logger.warning(e)
            else:
                raise
        logger.warning("CREATE: Created Table: {}".format(full_table_name))

    def setup_column_family(self, table_name, column_family, silent=True):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")

        con = self.connect()
        full_table_name = self.get_full_table_name(table_name)
        _SQL = """
        ALTER TABLE {} ADD COLUMN {} BLOB
        """.format(full_table_name, column_family)
        try:
            con.execute(_SQL)
        except OperationalError as e:
            if silent:
                logger.warning("CREATE CF: Ignoring existing family: {}".format(column_family))
                logger.warning(e)
            else:
                raise
        logger.warning("CREATE CF: Created Family: {}".format(column_family))

    def get_table(self, table_name):
        full_table_name = self.get_full_table_name(table_name)
        return SQLiteTable(self.db_connection, full_table_name)


class SQLiteTable(StorageTable):
    def __init__(self, con, table):
        self.con = con
        self.table = table

    @classmethod
    def split_column(cls, column_name):
        fam, col = column_name.split(":", 1)
        return fam, col

    @classmethod
    def build_column(cls, fam, col):
        return "{}:{}".format(fam, col)

    def _read_column_family(self, row_id, column_family):
        _SQL = "SELECT {} FROM {} WHERE k = ?;".format(column_family, self.table)
        cur = self.con.cursor()
        cur.execute(_SQL, (row_id,))
        res = cur.fetchone()
        if res:
            return json.loads(res[0])
        return None

    def _write_cells(self, row_id, column_family, values):
        old_col = self._read_column_family(row_id, column_family)
        if old_col is None:
            d = {}
        else:
            d = dict(old_col)
        for k, v in values.items():
            fam, col = self.split_column(k)
            assert fam == column_family
            d[col] = base64.b64encode(v).decode('ascii')
        _SQL = 'INSERT INTO {} (k, {}) VALUES(?, ?)'.format(self.table, fam)
        cur = self.con.cursor()
        raw_value = json.dumps(d)
        cur.execute(_SQL, (row_id, raw_value,))
        return cur.lastrowid

    def write_cell(self, row_id, column, value):
        fam, col = self.split_column(column)
        return self._write_cells(row_id, fam, {column: value})

    def read_row(self, row_id, column_families=None):
        if column_families is None:
            sel = "*"
        else:
            sel = ", ".join(["k"] + column_families)
        _SQL = "SELECT {} FROM {} WHERE k = ?;".format(sel, self.table)
        cur = self.con.cursor()
        cur.execute(_SQL, (row_id,))
        cols = [t[0] for t in cur.description]
        res = cur.fetchone()
        if res:
            out = {}
            for col_name, raw_val in zip(cols, res):
                if col_name == "k" or col_name == "row_meta":
                    continue
                if raw_val is None:
                    continue
                decoded = json.loads(raw_val)
                assert decoded
                for k, v in decoded.items():
                    val = base64.b64decode(v)
                    col = self.build_column(col_name, k)
                    out[col] = val
            return out
        raise KeyError("row {} not found".format(row_id))

    def delete_row(self, row_id, column_families=None):
        raise NotImplementedError

    def upsert_row(self, row_id, values):
        for k, v in values.items():
            fam, col = self.split_column(k)
        raise NotImplementedError

    def upsert_rows(self, row_upserts):
        raise NotImplementedError

    def row_generator(self, row_keys=None, start_key=None, end_key=None,
                      column_families=None, check_prefix=None):
        raise NotImplementedError

    def get_first_row(self, row_key_prefix, column_families=None):
        raise NotImplementedError

    def increment_counter(self, row_id, column, value):
        raise NotImplementedError