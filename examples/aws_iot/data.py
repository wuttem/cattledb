import datetime
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


client = CDBClient(engine=ENGINE, engine_options=ENGINE_OPTIONS, read_only=READ_ONLY, admin=ADMIN, table_prefix=TABLE_PREFIX)
client.service_init()

dt_from = pendulum.now().subtract(days=7)
dt_to = pendulum.now()

act = client.get_reader_activity("garden", dt_from, dt_to)
print(act)

temp = client.get_last_value("garden", "temperature")
print(temp[0])