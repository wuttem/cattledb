import os
import datetime
from cattledb.storage.connection import Connection
from cattledb.storage.models import TimeSeries
from cattledb.storage.helper import to_ts

# set env
os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8080"

# start emulator
# gcloud beta emulators bigtable start

db = Connection(project_id='test-system', instance_id='test')
db.create_tables(silent=True)
db.timeseries._create_metric("ph", silent=True)
db.timeseries._create_metric("temp", silent=True)
# exit()
#print(db.write_data_cell("1234", "raw:6", "hello world"))
#print(db.read_row("1234"))
#print(db.write_data_cell("1234", "raw:7", "hello world"))
#print(db.read_row("1234"))

# points = []
# ts = to_ts(datetime.datetime(2000, 1, 1, 0, 0))
# for j in range(10*144):
#     points.append((ts + j * 600, float(j % 6)))
# series = TimeSeries.from_list("ph", points)
# db.insert_timeseries("mydevice", series)

print(db.get("0000000050", "ph", 0, 5*24*60*60))

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
