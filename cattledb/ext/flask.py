#!/usr/bin/python
# coding: utf8

"""
A simple redis interface.
"""

import time
import datetime
import warnings

from cattledb.directclient import CDBClient

try:
    from flask import _app_ctx_stack as stack
    from flask import current_app
except ImportError:
    warnings.warn("unable to import flask, please install it")


class FlaskCattleDB(object):
    def __init__(self, app=None, dbconfig=None, table_prefix="cdb", read_only=True, admin=False):
        """
        Initialize this extension.

        :param obj app: The Flask application (optional).
        """
        self._db = None
        self.app = app
        if app is not None:
            self.init_app(app)

        self.table_prefix = table_prefix
        self.dbconfig = dbconfig
        self.read_only = True
        self.admin = admin

    def init_app(self, app, lazy=True):
        """
        Initialize this extension.

        :param obj app: The Flask application.
        """
        self.init_settings(app)
        if not lazy:
            self.connect(app)

    def init_settings(self, app):
        """Initialize all of the extension settings."""
        app.config.setdefault('ENGINE_OPTIONS', None)
        app.config.setdefault('ENGINE', None)
        app.config.setdefault('READ_ONLY', self.read_only)
        app.config.setdefault('ADMIN', self.admin)
        app.config.setdefault('TABLE_PREFIX', self.table_prefix)

    def connect(self, _app=None):
        if not _app:
            capp = current_app
        else:
            capp = _app

        engine = capp.config["ENGINE"]
        engine_options = capp.config["ENGINE_OPTIONS"]
        temp_db = CDBClient(
            engine=engine,
            engine_options=engine_options,
            table_prefix=capp.config["TABLE_PREFIX"],
            read_only=capp.config["READ_ONLY"],
            admin=capp.config["ADMIN"]
        )
        temp_db.service_init()
        self._db = temp_db
        return temp_db

    @property
    def connection(self):
        """
        Our database connection.

        This will be lazily created if this is the first time this is being
        accessed. This connection is reused for performance.
        """
        ctx = stack.top
        if ctx is not None:
            if self._db is None:
                self._db = self.connect()
                assert self._db
            return self._db
        raise RuntimeError("No App Context")

    @property
    def cattledb(self):
        return self.connection

    @property
    def client(self):
        return self.connection

    def __getattr__(self, atr):
        return getattr(self.connection, atr)
