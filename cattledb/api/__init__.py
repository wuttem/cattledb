#!/usr/bin/python
# coding: utf8

from .base import base_bp


def register(app):
    app.blueprint(base_bp)
