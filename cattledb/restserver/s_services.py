from sanic import Blueprint
from sanic.response import json


bp = Blueprint('base')


@bp.route('/')
def base_root(request):
    db = request.app.cdb
    return json(db.info())


@bp.route('/metrics')
def metrics(request):
    db = request.app.cdb
    connection = db.get_connection()
    out = []
    for m in connection.metric_definitions:
        out.append(m.to_dict())
    return json(out)


@bp.route('/events')
def events(request):
    db = request.app.cdb
    connection = db.get_connection()
    out = []
    for m in connection.event_definitions:
        out.append(m.to_dict())
    return json(out)


@bp.route('/database')
def database(request):
    db = request.app.cdb
    s = db.get_database_structure()
    return json(s)


@bp.route('/timeseries/<key:string>/<metric:string>/last_values')
async def last_values(request, key, metric):
    db = request.app.cdb
    s = await db.get_last_values(key, [metric])
    return json([x for x in s[0].get_serializable_iterator("iso")])
