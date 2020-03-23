#!/usr/bin/python
# coding: utf-8

import os
import click
import time

from ..core.helper import import_config_file
from ..core.models import MetricDefinition, EventDefinition, MetricType, EventSeriesType
from ..settings import default as _default_config
from ..directclient import CDBClient


@click.group()
@click.option('--configfile', '-c', type=click.Path(exists=True))
@click.option('--configclass', type=str)
@click.pass_context
def cli(ctx, configfile, configclass):
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


@cli.command()
@click.option('--force', is_flag=True)
@click.pass_context
def initdb(ctx, force):
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    assert db.init == False
    if force:
        click.confirm('Warning: This will init the database even if it already existed.', abort=True)
    # check for events and metrics
    if hasattr(config, "METRICS"):
        metr = config.METRICS
        click.echo("Loading {} metrics definitions".format(len(metr)))
        db.add_metric_definitions(metr)
    if hasattr(config, "EVENTS"):
        ev = config.EVENTS
        click.echo("Loading {} event definitions".format(len(ev)))
        db.add_event_definitions(ev)

    click.echo('Initializing database ...')
    db.database_init(silent=force)
    click.echo("Finished")


# @cli.command()
# @click.option('--force', is_flag=True)
# @click.pass_context
# def create_metrics(ctx, force):
#     db = ctx.obj["db"]
#     config = ctx.obj["config"]
#     if force:
#         click.confirm('This will create all metrics even if they existed before', abort=True)
#     db.service_init()
#     assert db.init == True
#     click.echo('Creating all metrics ...')
#     db.create_all_metrics(silent=force)
#     click.echo("Finished")


@cli.command()
@click.pass_context
def dbinfo(ctx):
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    db.service_init()
    assert db.init == True
    click.echo('Reading database structure ...')
    tables = db.read_database_structure()
    for t in tables:
        click.echo("# TABLE: {} ({})".format(t["name"], t["full_name"]))
        for cf in t["column_families"]:
            click.echo("- {}".format(cf))
    click.echo("# EVENTS")
    for e in db.event_definitions:
        click.echo(e)
    click.echo("# METRICS")
    for m in db.metric_definitions:
        click.echo(m)
    click.echo("# ALL GOOD!")


@cli.command()
@click.option('--metricid', prompt='Short metric identifier (2-6 chars)', type=str)
@click.option('--metricname', prompt='Metric name', type=str)
@click.option('--metrictype', prompt='Metric type', default="float",
              type=click.Choice(['float', 'dict'], case_sensitive=False))
@click.option('--delete/--nodelete', prompt='Allow delete operations', default=True,
              is_flag=True)
@click.pass_context
def newmetric(ctx, metricid, metricname, metrictype, delete):
    db = ctx.obj["db"]
    db.service_init()
    assert db.init == True
    click.echo('Creating a new metric definition ...')
    t = MetricType.DICTSERIES if metrictype == "dict" else MetricType.FLOATSERIES
    m = MetricDefinition(metricname, metricid, t, delete)
    db.new_metric_definition(m)
    click.echo('Created metric definition: {}'.format(metricname))


@cli.command()
@click.option('--eventname', prompt='Event name', type=str)
@click.option('--eventtype', prompt='Event type', default="daily",
              type=click.Choice(['daily', 'monthly'], case_sensitive=False))
@click.pass_context
def newevent(ctx, eventname, eventtype):
    db = ctx.obj["db"]
    db.service_init()
    assert db.init == True
    click.echo('Creating a new event definition ...')
    t = EventSeriesType.MONTHLY if eventtype == "monthly" else EventSeriesType.DAILY
    e = EventDefinition(eventname, t)
    db.new_event_definition(e)
    click.echo('Created event definition: {}'.format(eventname))


@cli.command()
@click.option('--port', type=int, default=5000)
@click.option('--debug/--nodebug', is_flag=True, default=True)
@click.pass_context
def runserver(ctx, port, debug):
    from ..restserver import _create_app
    config = ctx.obj["config"]
    app = _create_app(config)
    click.echo("Starting development REST server. DO NOT USE IN PRODUCTION!")
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=False)


@cli.command()
@click.argument("key")
@click.pass_context
def download_timeseries(ctx, key):
    db = ctx.obj["db"]
    db.service_init()
    assert db.init == True

    t1 = time.time()
    client = ctx.obj["client"]
    res = client.get_full_timeseries(key)
    file_name = os.path.realpath("test.csv")
    with open(file_name, "w") as fp:
        res.to_csv(fp)
    fs = os.path.getsize(file_name)
    fs = fs / 1024
    click.echo("Download finished. {:.2f} kb in {:.2f} seconds".format(fs, time.time()-t1))