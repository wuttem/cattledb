import os
from cattledb.restserver import create_app_by_configfile


configfile = os.environ.get("CATTLEDB_CONFIG", None)


app = create_app_by_configfile(configfile)
