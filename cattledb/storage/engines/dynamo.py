#!/usr/bin/python
# coding: utf-8

import os
import logging
import boto3

from collections import defaultdict, OrderedDict
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Binary

from .base import StorageEngine, StorageTable, NoTableError


logger = logging.getLogger(__name__)


class DynamoEngine(StorageEngine):
    def connect(self):
        if self.db_connection is None:
            params = {
                "aws_access_key_id": self.access_key_id,
                "aws_secret_access_key": self.secret_access_key,
                "region_name": self.region
            }
            if self.endpoint:
                params["endpoint_url"] = "http://{}".format(self.endpoint)
            self.db_connection =  boto3.resource('dynamodb', **params)
        return self.db_connection

    def setup_engine_options(self, engine_options):
        self.region = None
        self.access_key_id = None
        self.secret_access_key = None
        self.endpoint = None

        dynamo_emu = os.environ.get('DYNAMO_EMULATOR_HOST', None)
        if dynamo_emu or ("emulator" in engine_options and engine_options["emulator"]):
            self.access_key_id = "FAKEID"
            self.secret_access_key = "FAKEKEY"
            self.endpoint = dynamo_emu or engine_options["emulator"]
            self.region = "local"
            return
        elif "access_key_id" in engine_options and engine_options["access_key_id"] == "default":
            pass
        elif  "access_key_id" not in engine_options or "secret_access_key" not in engine_options:
            raise ValueError("missing access_key_id or secret_access_key for dynamo engine")
        else:
            self.access_key_id = engine_options["access_key_id"]
            self.secret_access_key = engine_options["secret_access_key"]

        if "region" not in engine_options:
            raise ValueError("missing region option for dynamo engine")
        else:
            self.region = engine_options["region"]

    def get_admin_table(self, table_name, store=None):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")
        return self.get_table(table_name, store=store)

    def get_table(self, table_name, store=None):
        full_table_name = self.get_full_table_name(table_name)
        return DynamoTable(self.db_connection, full_table_name, store)

    def disconnect(self):
        self.db_connection = None
        self.admin_connection = None

    def setup_table(self, table_name, silent=False, sorted=False):
        if not self.admin or self.read_only:
            raise RuntimeError("admin operations not allowed")

        con = self.connect()
        full_table_name = self.get_full_table_name(table_name)

        if sorted:
            ks = [{
                    'AttributeName': 'part',
                    'KeyType': 'HASH'  # Partition key
                }, {
                    'AttributeName': 'sort',
                    'KeyType': 'RANGE'  # Sort key
                }]
            ad = [{
                    'AttributeName': 'part',
                    'AttributeType': 'S'
                }, {
                    'AttributeName': 'sort',
                    'AttributeType': 'S'
                }]
        else:
            ks = [{'AttributeName': 'part', 'KeyType': 'HASH'}]
            ad = [{'AttributeName': 'part', 'AttributeType': 'S'}]

        try:
            table = con.create_table(
                TableName=full_table_name,
                KeySchema=ks,
                AttributeDefinitions=ad,
                BillingMode="PAY_PER_REQUEST"
            )
        except ClientError as e:
            if e.__class__.__name__ == 'ResourceInUseException' and silent:
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
        logger.warning("CREATE CF: Nothing happend for dynamo to add: {}".format(column_family))


class DynamoKey():
    def __init__(self, k, sorted=True):
        if not sorted:
            self._p = k
            self._s = None
            self._sorted = False
        else:
            self._sorted = True
            s = k.rsplit("#", 1)
            self._p = s[0]
            if len(s) == 2 and s[1]:
                self._s = s[1]
            else:
                self._s = None

    def __repr__(self):
        return "Key({})".format(self.to_str())

    @property
    def part(self):
        return self._p

    @property
    def sort(self):
        return self._s

    def has_sort(self):
        return bool(self._s)

    def is_full(self):
        if self._sorted:
            return bool(self._p) and bool(self._s)
        else:
            return True

    def to_dict(self):
        if self.has_sort():
            return {"part": self.part, "sort": self.sort}
        return {"part": self.part}

    def to_str(self):
        if self.has_sort():
            return "{}#{}".format(self._p, self._s)
        return self._p

    def add_prefix(self, s):
        self._p = "{}_{}".format(s, self._p)

    def _previous_key(self):
        assert len(self._p) > 2
        b = bytes(self._p, 'utf-8')
        self._p = self._p[:-1] + chr(max(b[-1]-1, 32))


class DynamoTable(StorageTable):
    def __init__(self, res, table_name, store):
        self._res = res
        self._table_name = table_name
        self._low_level = res.Table(table_name)
        self._store = store
        self.sorted = store.SORTED

    def _to_dynamo_key(self, k, cf=None):
        o = DynamoKey(k, self.sorted)
        if cf is not None:
            o.add_prefix(cf)
        return o

    def delete_row(self, row_id, column_families=None):
        if column_families is None:
            raise RuntimeError("cannot use column_family wildcard")

        for cf in column_families:
            k = self._to_dynamo_key(row_id, cf=cf)
            self._low_level.delete_item(Key=k.to_dict())

    def get_column_families(self):
        return self._store.all_column_families()
        # raise NotImplementedError("not supported for dynamo")

    def get_first_row(self, start_key, column_families=None, end_key=None):
        gen = self.row_generator(start_key=start_key, column_families=column_families, check_prefix=True, limit=1)
        try:
            return next(gen)
        except StopIteration:
            return None

        # l = list(gen)
        # print(l)
        # assert False


        # if column_families is None:
        #     raise RuntimeError("cannot use column_family wildcard")

        # if len(column_families) > 1:
        #     raise RuntimeError("cannot scan multiple column families in dynamo")

        # if not self.sorted:
        #     raise NotImplementedError("get_first_row ist not possible for unsorted tables")

        # cf = column_families[0]

        # start_key = self._to_dynamo_key(start_key, cf=cf)
        # #start_key._previous_key()
        # if end_key:
        #     end_key = self._to_dynamo_key(end_key, cf=cf)
        #     assert sta

        # params = {
        #     "Limit": 5,
        #     "ExclusiveStartKey": start_key.to_dict()
        # }

        # print(params)

        # res = self._low_level.scan(**params)
        # print(res)
        # print(res["Items"])
        # assert False

        # if end_key and end_key.has_sort() and start_key.has_sort():
        #     cond = Key('part').eq(start_key.part) & Key("sort").between(start_key.sort, end_key.sort)
        # elif end_key and end_key.has_sort():
        #     cond = Key('part').eq(start_key.part) & Key("sort").lte(end_key.sort)
        # elif start_key.has_sort():
        #     cond = Key('part').eq(start_key.part) & Key("sort").gte(start_key.sort)
        # else:
        #     cond = Key('part').eq(start_key.part)

        # params = {"KeyConditions": cond}

        # if column_families is not None:
        #     params["ProjectionExpression"] = ", ".join(column_families)

        # params["Limit"] = 1


        # res = self._low_level.query(**params)
        # print(res)
        # print(res["Items"])
        # assert False

    def increment_counter(self, row_id, column, value):
        if ":" in column:
            cf, k = column.parts()
        else:
            cf = "default"
            k = column
        
        rk = self._to_dynamo_key(row_id, cf=cf)

        update_expression = "ADD #key :incr".format(key=k)
        update_values = {":incr": value}
        update_names = {"#key": k}

        res = self._low_level.update_item(Key=rk.to_dict(),
                                          UpdateExpression=update_expression,
                                          ExpressionAttributeValues=update_values,
                                          ExpressionAttributeNames=update_names)

        return res

    def get_key_from_dict(self, d, cf="default"):
        # try to remove prefix
        p = d["part"]
        h = "{}_".format(cf)
        if p.startswith(h):
            p = p[len(h):]
        if self.sorted:
            s = d["sort"]
            return DynamoKey("{}#{}".format(p, s))
        return DynamoKey(p)

    def item_to_dict(self, d, cf):
        d_out = OrderedDict()
        for k, v in sorted(d.items()):
            if k != "part" and k !="sort":
                if isinstance(v, Binary):
                    d_out["{}:{}".format(cf, k)] = v.value
                else:
                    d_out["{}:{}".format(cf, k)] = v
        return d_out

    def read_row(self, row_id, column_families=None):
        if column_families is None:
            raise RuntimeError("cannot use column_family wildcard")

        out = {}

        for cf in column_families:
            k = self._to_dynamo_key(row_id, cf=cf)
            assert k.is_full()

            params = {
                "Key": k.to_dict()
            }

            try:
                res = self._low_level.get_item(**params)
            except ClientError as e:
                if e.__class__.__name__ == 'ResourceNotFoundException':
                    logger.warning(e)
                    raise NoTableError('ResourceNotFoundException for table {}'.format(self._table_name))
                else:
                    raise

            if "Item" in res and res["Item"]:
                out.update(self.item_to_dict(res["Item"], cf))

        if len(out) < 1:
            raise KeyError("row {} not found".format(row_id))
        return out

    def _get_items(self, row_keys, column_families):
        rows = defaultdict(dict)

        for cf in column_families:
            req = []
            for rk in row_keys:
                k = self._to_dynamo_key(rk, cf=cf)
                assert k.is_full()
                req.append(k.to_dict())

            params = {"RequestItems": {
                self._table_name: { "Keys" : req}
            }}

            res = self._res.batch_get_item(**params)
            items = res["Responses"][self._table_name]

            while res['UnprocessedKeys']:
                # todo error handling
                raise RuntimeError("there are unprocessed keys")

            for i in items:
                rk = self.get_key_from_dict(i, cf).to_str()
                rows[rk].update(self.item_to_dict(i, cf))

        return list(rows.items())

    def row_generator(self, row_keys=None, start_key=None, end_key=None,
                      column_families=None, check_prefix=False, limit=None):
        if column_families is None:
            raise RuntimeError("cannot use column_family wildcard")
        if row_keys is None and start_key is None:
            raise ValueError("use row_keys or start_key parameter")
        if start_key is not None and (end_key is None and not check_prefix):
            raise ValueError("use start_key together with end_key or check_prefix")

        # get this with a batch read
        if row_keys:
            rows = self._get_items(row_keys, column_families=column_families)
            yield from sorted(rows, key=lambda x: x[0])
            return

        for cf in column_families:
            sk = self._to_dynamo_key(start_key, cf=cf)
            ek = None
            if end_key:
                ek = self._to_dynamo_key(end_key, cf=cf)
                if ek.part != sk.part:
                    raise RuntimeError("cannot get a range in with different partition keys")

            # get start and end keys
            if ek and ek.has_sort() and sk.has_sort():
                cond = Key('part').eq(sk.part) & Key("sort").between(sk.sort, ek.sort)
            elif ek and ek.has_sort():
                cond = Key('part').eq(sk.part) & Key("sort").lte(ek.sort)
            elif sk.has_sort():
                cond = Key('part').eq(sk.part) & Key("sort").gte(sk.sort)
            else:
                cond = Key('part').eq(sk.part)
            params = {"KeyConditionExpression": cond}

            if limit is not None:
                params["Limit"] = limit

            count = 0
            res = self._low_level.query(**params)
            count += res["Count"]

            while (('LastEvaluatedKey' in res and res['LastEvaluatedKey']) and
                   not (limit is not None and count >= limit)):
                # todo error handling
                raise RuntimeError("there are unprocessed keys")

            for i in res["Items"]:
                rk = self.get_key_from_dict(i, cf).to_str()
                yield (rk, self.item_to_dict(i, cf))

    def write_cell(self, row_id, column, value):
        return self.upsert_row(row_id, {column: value})

    def upsert_rows(self, row_upserts):
        results = []
        for row in row_upserts:
            results.append(self.upsert_row(row.row_key, row.cells))
        return results

    def upsert_row(self, row_id, values):
        for k, expr, val, nam in self.get_update_params(row_id, values):
            res = self._low_level.update_item(Key=k.to_dict(),
                                              UpdateExpression=expr,
                                              ExpressionAttributeValues=val,
                                              ExpressionAttributeNames=nam)
        return res

    def get_update_params(self, row_id, values):
        rows = defaultdict(dict)
        for key, val in values.items():
            if ":" in key:
                cf, k = key.parts()
            else:
                cf = "default"
                k = key
            rows[cf][k] = val

        out = []
        for cf, values in rows.items():
            rk = self._to_dynamo_key(row_id, cf=cf)

            update_expression = ["set "]
            update_values = dict()
            update_names = dict()
            for key, val in values.items():
                update_expression.append(" #{key} = :{key},".format(key=key))
                update_values[":{key}".format(key=key)] = val
                update_names["#{key}".format(key=key)] = key
            
            out.append((rk, "".join(update_expression)[:-1], update_values, update_names))
        return out