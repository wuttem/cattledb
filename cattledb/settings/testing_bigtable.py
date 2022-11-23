import os

from cattledb.settings.testing_base import *

os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8080"


TESTING = True
DEBUG = True

ENGINE = "bigtable"
ENGINE_OPTIONS = {
    "credentials": None,
    "project_id": "prj1",
    "instance_id": "ins1",
    "emulator": True,
    "assert_limits": True
}
