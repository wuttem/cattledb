from random import random
import pendulum
import random

from flask import Flask, render_template
from flask_dataview import FlaskDataViews, RangeTimeSeries
from cattledb.ext.flask import FlaskCattleDB


class MyConf:
    TABLE_PREFIX = "weatherdata"
    ENGINE = "dynamo"
    ENGINE_OPTIONS = {
        "assert_limits": True,
        "region": "eu-central-1",
        "access_key_id": "default"
    }

e = FlaskDataViews()
app = Flask(__name__, template_folder=".")
db = FlaskCattleDB()
db.init_app(app)
e.init_app(app)
app.config.from_object(MyConf)


class MySeries(RangeTimeSeries):
    def get_range(self):
        d1 = pendulum.now("utc").subtract(days=14)
        d2 = pendulum.now("utc")
        return (d1, d2)

    def get_data_range(self, dt_from, dt_to):
        out = []
        res = db.cattledb.get_timeseries("garden", [self.name], dt_from, dt_to)[0]
        print(res)
        for p in res:
            out.append((p.dt.isoformat(), p.value))
        return out


@app.route('/')
def last_value():
    return "{}".format(db.cattledb.get_last_value("garden", "temperature"))


@app.route('/chart', methods=['POST', 'GET'])
def home():
    data = [MySeries("temperature"), MySeries("humidity")]

    mychart = e.basechart("myid1", "My Chart", series=data)
    if mychart.is_post_request():
        return mychart.data()
    return render_template("template.html", chart=mychart)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=False)