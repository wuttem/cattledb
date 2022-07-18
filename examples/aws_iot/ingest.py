import pendulum
from cattledb.directclient import CDBClient


TABLE_PREFIX = "weatherdata"
READ_ONLY = False
ADMIN = True
ENGINE = "dynamo"
ENGINE_OPTIONS = {
    "assert_limits": True,
    "region": "eu-central-1",
    "access_key_id": "default"
}


testevent = {
    "time": "2022-07-15 09:00:46",
    "model": "Cotech-367959",
    "id": 45,
    "battery_ok": 1,
    "temperature_F": 67.9,
    "humidity": 76,
    "rain_mm": 2.4,
    "wind_dir_deg": 358,
    "wind_avg_m_s": 2.6,
    "wind_max_m_s": 3,
    "light_lux": 69627,
    "uv": 251,
    "mic": "CRC"
}


def lambda_handler(event, context):
    # check for correct station
    if event["model"] != "Cotech-367959":
        return False

    dt = pendulum.parse(event["time"], tz='Europe/Paris')
    cur = pendulum.now()
    # check up to date
    if abs(cur.diff(dt).in_minutes()) > 100:
        return False

    data = [
        {"key": "garden", "metric": "temperature", "data": [(dt, (event["temperature_F"]-32.0)*5.0/9.0)]},
        {"key": "garden", "metric": "humidity", "data": [(dt, event["humidity"])]},
        {"key": "garden", "metric": "rain", "data": [(dt, event["rain_mm"])]},
        {"key": "garden", "metric": "wind_dir", "data": [(dt, event["wind_dir_deg"])]},
        {"key": "garden", "metric": "wind", "data": [(dt, event["wind_avg_m_s"])]},
        {"key": "garden", "metric": "light", "data": [(dt, event["light_lux"])]},
        {"key": "garden", "metric": "battery", "data": [(dt, event["battery_ok"])]},
    ]

    client = CDBClient(engine=ENGINE, engine_options=ENGINE_OPTIONS, read_only=READ_ONLY, admin=ADMIN, table_prefix=TABLE_PREFIX)
    client.service_init()
    client.put_timeseries_multi(data)
    client.incr_activity("garden", "garden", dt)

    return True


#lambda_handler(testevent, None)