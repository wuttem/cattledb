#!/usr/bin/python
# coding: utf-8

import click

from ..core.helper import import_config_file
from ..settings import default as _default_config
from ..directclient import CDBClient


@click.group()
@click.option('--configfile', '-c', type=click.Path(exists=True))
@click.option('--configclass', type=str)
@click.pass_context
def cli(ctx, configfile, configclass):
    """CattleDB Command Line Tool"""
    ctx.ensure_object(dict)
    if configfile:
        _imported = import_config_file(configfile)
        if configclass:
            config = getattr(_imported, configclass)
        else:
            config = _imported
        click.echo("Using Config: {}".format(configfile))
    else:
        config = _default_config
        click.echo("Using Default Config")

    con = CDBClient.from_config(config)
    ctx.obj["client"] = con
    ctx.obj["db"] = con.db
    ctx.obj["config"] = config


from .base import initdb, dbinfo, newmetric, newevent, runserver, download_timeseries

cli.add_command(initdb)
cli.add_command(dbinfo)
cli.add_command(newmetric)
cli.add_command(newevent)
cli.add_command(runserver)
cli.add_command(download_timeseries)
