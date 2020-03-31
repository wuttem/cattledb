import pendulum

from flask import jsonify, Blueprint, current_app, abort
from ..core.models import FastDictTimeseries

bp = Blueprint('base', __name__)


@bp.route('/')
def base_root():
    db = current_app.cdb
    return jsonify(db.info())


@bp.route('/metrics')
def metrics():
    db = current_app.cdb
    connection = db.get_connection()
    out = []
    for m in connection.metric_definitions:
        out.append(m.to_dict())
    return jsonify(out)


@bp.route('/events')
def events():
    db = current_app.cdb
    connection = db.get_connection()
    out = []
    for m in connection.event_definitions:
        out.append(m.to_dict())
    return jsonify(out)


@bp.route('/database')
def database():
    db = current_app.cdb
    s = db.get_database_structure()
    return jsonify(s)


@bp.route('/timeseries/<key>/<metric>/last_value')
def last_value(key, metric):
    db = current_app.cdb
    s = db.get_last_value(key, metric)
    return jsonify([x for x in s.get_serializable_iterator("iso")])


@bp.route('/timeseries/<key>/<metric>/<int:days>days')
def metric_days(key, metric, days):
    db = current_app.cdb
    t = pendulum.now("utc").add(hours=1)
    f = pendulum.now("utc").subtract(days=days)
    s = db.get_timeseries(key, [metric], f, t)[0]
    return jsonify([x for x in s.get_serializable_iterator("iso")])


@bp.route('/timeseries/<key>/<int:days>days')
def days(key, days):
    db = current_app.cdb
    t = pendulum.now("utc").add(hours=1)
    f = pendulum.now("utc").subtract(days=days)
    res = db.get_all_metrics(key, f, t)
    if res:
        return jsonify([x for x in res.get_serializable_iterator("iso")])
    return jsonify(None), 200


@bp.route('/timeseries/<key>/full')
def full_download(key):
    db = current_app.cdb
    res = db.get_full_timeseries(key)
    if res:
        return jsonify([x for x in res.get_serializable_iterator("iso")])
    return jsonify(None), 200
