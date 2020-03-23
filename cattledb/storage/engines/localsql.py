#!/usr/bin/python
# coding: utf-8

import sqlite3
import logging
import os
import json
import base64
import struct

from collections import defaultdict, OrderedDict
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
            self.db_connection = None

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
        self.disconnect()
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
        self.disconnect()
        logger.warning("CREATE CF: Created Family: {}".format(column_family))

    def get_table(self, table_name):
        con = self.connect()
        full_table_name = self.get_full_table_name(table_name)
        return SQLiteTable(con, full_table_name)

    def get_admin_table(self, table_name):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")
        return self.get_table(table_name)


class SQLiteTable(StorageTable):
    def __init__(self, con, table):
        self.con = con
        self.table = table

    @classmethod
    def split_column(cls, column_name):
        fam, col = column_name.split(":", 1)
        return fam, col

    def decode_row_data(self, row_data, column_names):
        d = OrderedDict()
        for col_name, raw_val in zip(column_names, row_data):
            if col_name == "k" or col_name == "row_meta":
                continue
            if raw_val is None:
                continue
            decoded = json.loads(raw_val)
            assert decoded
            for k, v in decoded.items():
                val = base64.b64decode(v)
                col = self.build_column(col_name, k)
                d[col] = val
        return d

    @classmethod
    def build_column(cls, fam, col):
        return "{}:{}".format(fam, col)

    def _read_column_family(self, row_id, column_family):
        _SQL = "SELECT {} FROM {} WHERE k = ?;".format(column_family, self.table)
        cur = self.con.cursor()
        cur.execute(_SQL, (row_id,))
        res = cur.fetchone()
        if res and res[0]:
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
        # Below is the SQLite >3.24 version
        # _SQL = 'INSERT INTO {} (k, {}) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET {} = ? WHERE k = ?'.format(self.table, fam, fam)
        _SQL_UPDATE = 'UPDATE {} SET {} = ? WHERE k = ?;'.format(self.table, fam)
        _SQL_INSERT = 'INSERT INTO {} (k, {}) SELECT ?, ? WHERE (Select Changes() = 0);'.format(self.table, fam)
        cur = self.con.cursor()
        raw_value = json.dumps(d)
        # try update
        cur.execute(_SQL_UPDATE, (raw_value, row_id,))
        # try insert
        cur.execute(_SQL_INSERT, (row_id, raw_value,))
        self.con.commit()
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
            return self.decode_row_data(res, cols)
        raise KeyError("row {} not found".format(row_id))

    def delete_row(self, row_id, column_families=None):
        cur = self.con.cursor()
        if column_families is None:
            _SQL = "DELETE FROM {} WHERE k = ?;".format(self.table)
            cur.execute(_SQL, (row_id,))
        else:
            ups = ", ".join(["{} = null".format(c) for c in column_families])
            _SQL = "UPDATE {} SET {} WHERE k = ?;".format(self.table, ups)
            cur.execute(_SQL, (row_id,))
        self.con.commit()

    def upsert_row(self, row_id, values):
        inserts = defaultdict(dict)
        # sort by family
        for k, v in values.items():
            fam, col = self.split_column(k)
            inserts[fam][k] = v
        for fam, vals in inserts.items():
            self._write_cells(row_id, fam, vals)
        return True

    def upsert_rows(self, row_upserts):
        res = []
        for r in row_upserts:
            res.append(self.upsert_row(r.row_key, r.cells))
        return res

    def row_generator(self, row_keys=None, start_key=None, end_key=None,
                      column_families=None, check_prefix=None):
        if row_keys is None and start_key is None:
            raise ValueError("use row_keys or start_key parameter")
        if start_key is not None and (end_key is None and check_prefix is None):
            raise ValueError("use start_key together with end_key or check_prefix")

        if column_families is None:
            sel = "*"
        else:
            sel = ", ".join(["k"] + column_families)

        params = []
        if row_keys is not None:
            filter_terms = []
            for r in row_keys:
                filter_terms.append("k = ?")
                params.append(r)
            filter = " OR ".join(filter_terms)
        elif start_key is not None:
            filter = "k >= ?"
            params.append(start_key)
            if end_key is not None:
                filter = filter + " AND k <= ?"
                params.append(end_key)
        else:
            raise ValueError("use row_keys or start_key parameter")

        _SQL = "SELECT {} FROM {} WHERE {} ORDER BY k;".format(sel, self.table, filter)
        cur = self.con.cursor()
        cur.execute(_SQL, tuple(params))
        cols = [t[0] for t in cur.description]
        # first should be key
        assert cols[0] == "k"

        for row in cur:
            curr_row_dict = self.decode_row_data(row, cols)
            rk = row[0]
            if len(curr_row_dict) == 0:
                continue
            if check_prefix:
                if not rk.startswith(check_prefix):
                    break
            yield (rk, curr_row_dict)

    def get_first_row(self, start_key, column_families=None, end_key=None):
        if column_families is None:
            sel = "*"
        else:
            sel = ", ".join(["k"] + column_families)

        filter = "k >= ?"
        _SQL = "SELECT {} FROM {} WHERE {} ORDER BY k;".format(sel, self.table, filter)
        cur = self.con.cursor()
        cur.execute(_SQL, (start_key,))
        cols = [t[0] for t in cur.description]
        # first should be key
        assert cols[0] == "k"

        for row in cur:
            curr_row_dict = self.decode_row_data(row, cols)
            rk = row[0]
            if len(curr_row_dict) == 0:
                continue
            if not rk.startswith(start_key):
                break
            return (rk, curr_row_dict)

    def increment_counter(self, row_id, column, value):
        fam, col = self.split_column(column)
        try:
            d = self.read_row(row_id, column_families=[fam])
            b = d.get(column, None)
            if b is None:
                old_value = 0
            else:
                old_value = struct.Struct('>q').unpack(b)[0]
        except KeyError:
            old_value = 0

        if old_value:
            new_value = old_value + value
        else:
            new_value = 0 + value
        d = struct.Struct('>q').pack(new_value)
        self.write_cell(row_id, column, d)
        return new_value

    def get_column_families(self):
        _SQL = "PRAGMA table_info('{}');".format(self.table)
        cur = self.con.cursor()
        cur.execute(_SQL)
        columns = [r[1] for r in cur if r[1] not in ("row_meta", "k")]
        return columns
