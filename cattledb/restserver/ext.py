#!/usr/bin/python
# coding: utf-8


import warnings


from ..directclient import CDBClient


try:
    from flask import _app_ctx_stack as stack
    from flask import current_app
except ImportError:
    warnings.warn("unable to import flask, please install it")


class FlaskCDB(object):
    def __init__(self, engine=None, engine_options=None, read_only=False,
                 admin=False, table_prefix="cdb", app=None):
        """
        Initialize this extension.

        :param obj app: The Flask application (optional).
        """
        self._db = None
        self.engine = engine
        self.engine_options = engine_options
        self.read_only = read_only
        self.admin = admin
        self.table_prefix = table_prefix

        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """
        Initialize this extension.

        :param obj app: The Flask application.
        """
        self.init_settings(app)

    def init_settings(self, app):
        """Initialize all of the extension settings."""
        app.config.setdefault('CATTLEDB_ENGINE', self.engine)
        app.config.setdefault('CATTLEDB_ENGINE_OPTIONS', self.engine_options)
        app.config.setdefault('CATTLEDB_READ_ONLY', self.read_only)
        app.config.setdefault('CATTLEDB_ADMIN', self.admin)
        app.config.setdefault('CATTLEDB_TABLE_PREFIX', self.table_prefix)
        app.config.setdefault('CATTLEDB_CLIENT_CLASS', CDBClient)

    def _connect(self, _app):
        if _app.config["CATTLEDB_CLIENT_CLASS"] and callable(_app.config["CATTLEDB_CLIENT_CLASS"]):
            cl = _app.config["CATTLEDB_CLIENT_CLASS"]
        else:
            cl = CDBClient
        _db = cl(engine=_app.config["CATTLEDB_ENGINE"],
                 engine_options=_app.config["CATTLEDB_ENGINE_OPTIONS"],
                 read_only=_app.config["CATTLEDB_READ_ONLY"],
                 admin=_app.config["CATTLEDB_ADMIN"],
                 table_prefix=_app.config["CATTLEDB_TABLE_PREFIX"])
        _db.service_init()
        return _db

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
                self._db = self._connect(current_app)
            return self._db
        raise RuntimeError("No App Context")

    def warmup(self, _app):
        if self._db is None:
            self._db = self._connect(_app)

    def __getattr__(self, attr):
        if hasattr(self.connection, attr):
            _proxy = getattr(self.connection, attr)
            if callable(_proxy):
                return _proxy
        raise AttributeError
