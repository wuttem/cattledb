#!/usr/bin/python
# coding: utf-8

import os
import logging
import struct
import time

from collections import OrderedDict

from google.cloud import bigtable
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter, FamilyNameRegexFilter, RowFilterChain, RowFilterUnion, RowKeyRegexFilter
from google.cloud.bigtable.row_set import RowSet
from google.cloud._helpers import _to_bytes
from google.cloud.bigtable.column_family import MaxVersionsGCRule
# from google.oauth2 import service_account


from .base import StorageEngine, StorageTable


logger = logging.getLogger(__name__)


class BigtableEngine(StorageEngine):
    def setup_table(self, table_name, silent=False):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")

        ad = self.get_admin_connection()

        tables_before = [t.table_id for t in ad.list_tables()]
        logger.debug("CREATE: Existing Tables: {}".format(tables_before))

        full_table_name = self.get_full_table_name(table_name)
        if silent and full_table_name in tables_before:
            logger.warning("CREATE: TABLE {} ALREADY EXISTING".format(full_table_name))
            return

        low_table = ad.table(full_table_name)
        low_table.create()

        logger.warning("CREATE: Created Table: {}".format(full_table_name))

        table = BigtableTable(low_table)
        return table

    def setup_column_family(self, table_name, column_family, silent=False):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")

        ad = self.get_admin_connection()
        full_table_name = self.get_full_table_name(table_name)
        t = ad.table(full_table_name)
        families_before = t.list_column_families()
        logger.debug("CREATE CF: Existing Families: {}".format(families_before))
        if silent and column_family in families_before:
            logger.warning("CREATE CF: Ignoring existing family: {}".format(column_family))
            return
        cf1 = t.column_family(column_family, gc_rule=MaxVersionsGCRule(1))
        cf1.create()
        time.sleep(0.25)
        logger.warning("CREATE CF: Created Family: {}".format(column_family))
        return t

    def get_admin_connection(self):
        if not self.admin or self.read_only:
            raise RuntimeError("creation of admin connection not allowed")
        if self.admin_connection is None:
            self.admin_connection = bigtable.Client(project=self.project_id, admin=True,
                                                    credentials=self.credentials).instance(self.instance_id)
            logger.warning("Created new admin connection")
        return self.admin_connection

    def get_table(self, table_name):
        full_table_name = self.get_full_table_name(table_name)
        return BigtableTable(self.db_connection.table(full_table_name))

    def get_admin_table(self, table_name):
        eng = self.get_admin_connection()
        full_table_name = self.get_full_table_name(table_name)
        return BigtableTable(eng.table(full_table_name))

    def setup_engine_options(self, engine_options):
        self.credentials = None
        self.instance_id = None
        self.project_id = None

        bigtable_emu = os.environ.get('BIGTABLE_EMULATOR_HOST', None)
        if bigtable_emu or ("emulator" in engine_options and engine_options["emulator"]):
            self.credentials = AnonymousCredentials()
        elif "credentials" not in engine_options:
            raise ValueError("missing credentials option for bigtable engine")
        else:
            self.credentials = engine_options["credentials"]
            # use this if credentials is a path to a key.json service account
            # self.credentials = service_account.Credentials.from_service_account_file(engine_options["credentials"])

        if "project_id" not in engine_options:
            raise ValueError("missing project_id option for bigtable engine")
        else:
            self.project_id = engine_options["project_id"]

        if "instance_id" not in engine_options:
            raise ValueError("missing instance_id option for bigtable engine")
        else:
            self.instance_id = engine_options["instance_id"]

    def connect(self):
        if self.db_connection is None:
            self.db_connection = bigtable.Client(project=self.project_id, admin=False, read_only=self.read_only,
                                                 credentials=self.credentials).instance(self.instance_id)
        return self.db_connection

    def disconnect(self):
        self.db_connection = None
        self.admin_connection = None


class BigtableTable(StorageTable):
    def __init__(self, low_level):
        self._low_level = low_level

    @classmethod
    def partial_row_to_ordered_dict(cls, row_data):
        result = OrderedDict()
        for column_family_id, columns in row_data._cells.items():
            for column_qual, cells in columns.items():
                key = _to_bytes(column_family_id) + b":" + _to_bytes(column_qual)
                result[key.decode("utf-8")] = cells[0].value
        return result

    @classmethod
    def partial_row_to_dict(cls, row_data):
        result = {}
        for cf, data in row_data.to_dict().items():
            result[cf.decode("utf-8")] = data[0].value
        return result

    def write_cell(self, row_id, column, value):
        row = self._low_level.direct_row(row_id.encode("utf-8"))
        column_family, col = column.split(":", 1)
        row.set_cell(column_family.encode("utf-8"), col.encode("utf-8"), value)
        row.commit()
        return 1

    def read_row(self, row_id, column_families=None):
        filters = [CellsColumnLimitFilter(1)]
        if column_families is not None:
            c_filters = []
            for c in column_families:
                c_filters.append(FamilyNameRegexFilter(c))
            if len(c_filters) == 1:
                filters.append(c_filters[0])
            elif len(c_filters) > 1:
                filters.append(RowFilterUnion(c_filters))
        if len(filters) > 1:
            filter_ = RowFilterChain(filters=filters)
        else:
            filter_ = filters[0]

        res = self._low_level.read_row(row_id.encode("utf-8"), filter_=filter_)
        if res is None:
            raise KeyError("row {} not found".format(row_id))
        return self.partial_row_to_dict(res)

    def delete_row(self, row_id, column_families=None):
        row = self._low_level.direct_row(row_id.encode("utf-8"))
        if column_families is None:
            row.delete()
        else:
            for c in column_families:
                row.delete_cells(c.encode("utf-8"), row.ALL_COLUMNS)
        row.commit()
        return 1

    def upsert_row(self, row_id, values):
        row = self._low_level.direct_row(row_id)
        for c, value in values.items():
            column_family, col = c.split(":", 1)
            row.set_cell(column_family.encode("utf-8"), col.encode("utf-8"), value)
        response = self._low_level.mutate_rows([row])[0]
        if response.code != 0:
            raise ValueError("Bigtable upsert failed with: {} - {}".format(response.code, response.message))
        return response

    def upsert_rows(self, row_upserts):
        rows = []
        for r in row_upserts:
            row = self._low_level.direct_row(r.row_key)
            for c, value in r.cells.items():
                column_family, col = c.split(":", 1)
                row.set_cell(column_family.encode("utf-8"), col.encode("utf-8"), value)
            rows.append(row)
        responses = self._low_level.mutate_rows(rows)
        for r in responses:
            if r.code != 0:
                raise ValueError("Bigtable upsert failed with: {} - {}".format(r.code, r.message))
        return responses

    def row_generator(self, row_keys=None, start_key=None, end_key=None,
                      column_families=None, check_prefix=None):
        if row_keys is None and start_key is None:
            raise ValueError("use row_keys or start_key parameter")
        if start_key is not None and (end_key is None and check_prefix is None):
            raise ValueError("use start_key together with end_key or check_prefix")

        filters = [CellsColumnLimitFilter(1)]
        if column_families is not None:
            c_filters = []
            for c in column_families:
                c_filters.append(FamilyNameRegexFilter(c))
            if len(c_filters) == 1:
                filters.append(c_filters[0])
            elif len(c_filters) > 1:
                filters.append(RowFilterUnion(c_filters))
        if len(filters) > 1:
            filter_ = RowFilterChain(filters=filters)
        else:
            filter_ = filters[0]

        row_set = RowSet()
        if row_keys:
            for r in row_keys:
                row_set.add_row_key(r)
        else:
            row_set.add_row_range_from_keys(start_key=start_key, end_key=end_key,
                                            start_inclusive=True, end_inclusive=True)

        generator = self._low_level.read_rows(filter_=filter_, row_set=row_set)

        i = -1
        for rowdata in generator:
            i += 1
            if rowdata is None:
                if row_keys:
                    yield (row_keys[i], {})
                continue
            rk = rowdata.row_key.decode("utf-8")
            if check_prefix:
                if not rk.startswith(check_prefix):
                    break
            curr_row_dict = self.partial_row_to_ordered_dict(rowdata)
            yield (rk, curr_row_dict)

    def get_first_row(self, start_key, column_families=None, end_key=None):
        filters = [CellsColumnLimitFilter(1)]
        if column_families is not None:
            c_filters = []
            for c in column_families:
                c_filters.append(FamilyNameRegexFilter(c))
            if len(c_filters) == 1:
                filters.append(c_filters[0])
            elif len(c_filters) > 1:
                filters.append(RowFilterUnion(c_filters))
        if len(filters) > 1:
            filter_ = RowFilterChain(filters=filters)
        else:
            filter_ = filters[0]

        row_set = RowSet()
        row_set.add_row_range_from_keys(start_key=start_key, start_inclusive=True, end_key=end_key)

        generator = self._low_level.read_rows(filter_=filter_, row_set=row_set)

        i = -1
        for rowdata in generator:
            i += 1
            # if rowdata is None:
            #     continue
            rk = rowdata.row_key.decode("utf-8")
            if end_key is None and not rk.startswith(start_key):
                break
            curr_row_dict = self.partial_row_to_dict(rowdata)
            return (rk, curr_row_dict)

    # def read_rows(self, row_keys=None, start_key=None, end_key=None,
    #               column_families=None):
    #     generator = self.row_generator(row_keys=row_keys, start_key=start_key, end_key=end_key,
    #                                    column_families=column_families)

    #     result = []
    #     for rk, data in generator:
    #         result.append((rk, data))
    #     return result

    # Taken from google-bigtable-happybase
    def increment_counter(self, row_id, column, value):
        """Atomically increment a counter column.
        This method atomically increments a counter column in ``row``.
        If the counter column does not exist, it is automatically initialized
        to ``0`` before being incremented.
        :type row: str
        :param row: Row key for the row we are incrementing a counter in.
        :type column: str
        :param column: Column we are incrementing a value in; of the
                       form ``fam:col``.
        :type value: int
        :param value: Amount to increment the counter by. (If negative,
                      this is equivalent to decrement.)
        :rtype: int
        :returns: Counter value after incrementing.
        """
        row = self._low_level.append_row(row_id.encode("utf-8"))
        column_family_id, column_qualifier = column.split(':')
        row.increment_cell_value(column_family_id.encode("utf-8"),
                                 column_qualifier.encode("utf-8"), value)
        modified_cells = row.commit()

        inner_keys = list(modified_cells[column_family_id].keys())
        if not inner_keys:
            raise KeyError(column_qualifier)

        if isinstance(inner_keys[0], bytes):
            column_cells = modified_cells[
                column_family_id][column_qualifier.encode("latin-1")]
        elif isinstance(inner_keys[0], str):
            column_cells = modified_cells[
                column_family_id][column_qualifier]
        else:
            raise KeyError(column_qualifier)

        # Make sure there is exactly one cell in the column.
        if len(column_cells) != 1:
            raise ValueError('Expected server to return one modified cell.')
        column_cell = column_cells[0]
        # Get the bytes value from the column and convert it to an integer.
        bytes_value = column_cell[0]
        int_value, = struct.Struct('>q').unpack(bytes_value)
        return int_value

    def get_column_families(self):
        return list(self._low_level.list_column_families().keys())
