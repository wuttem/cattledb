import os
import datetime
from cattledb.storage.connection import Connection
from cattledb.directclient import CDBClient
from cattledb.storage.models import TimeSeries
from cattledb.core.helper import to_ts
from cattledb.settings import DevelopmentConfig

# set env
os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8080"

# start emulator
# gcloud beta emulators bigtable start


client = CDBClient(engine=DevelopmentConfig.ENGINE, engine_options=DevelopmentConfig.ENGINE_OPTIONS,
                   table_prefix=DevelopmentConfig.TABLE_PREFIX, read_only=False, admin=True)
db = client.db

#db.restore_configuration()
#db.init_service()

#db.add_metric_definitions(DevelopmentConfig.METRICS)
#db.database_init(silent=True)
db.service_init()
#db.timeseries._create_metric("ph", silent=True)
#db.timeseries._create_metric("temp", silent=True)

print(db.read_database_structure())

# exit()
#print(db.write_data_cell("1234", "raw:6", "hello world"))
#print(db.read_row("1234"))
#print(db.write_data_cell("1234", "raw:7", "hello world"))
#print(db.read_row("1234"))

points = []
ts = to_ts(datetime.datetime(2000, 1, 1, 0, 0))
for j in range(10*144):
    points.append((ts + j * 600, float(j % 6)))
series = TimeSeries("mydevice", "ph", points)
db.timeseries.insert_timeseries(series)

print(db.timeseries.get("mydevice", "ph", 0, 5*24*60*60))

# print(db.read_row("mydevice#30004940"))
# client = bigtable.Client(project='smaxtec-system', admin=True)
# instances = client.list_instances()
# print(instances)
# instance = instances[0][0]
# instance.reload()
# print(instance.display_name)
# print(instance.list_tables())
# table = instance.table("meinetolletable")
# column_family = table.column_family("raw")
# column_family.create()
