import asyncio

from flask import jsonify, Blueprint, current_app


bp = Blueprint('base', __name__)


@bp.route('/')
def base_root():
    db = current_app.cdb
    return jsonify({'name': "cattledb", 'alive': True, "db": str(db)})


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


@bp.route('/timeseries/<key>.<metric>/last_values')
def last_values(key, metric):
    db = current_app.cdb
    s = db.get_last_values(key, [metric])[0]
    return jsonify([x for x in s.get_serializable_iterator("iso")])
