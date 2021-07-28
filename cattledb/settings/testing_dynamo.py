import os

from cattledb.settings.default import *

os.environ["DYNAMO_EMULATOR_HOST"] = "localhost:8000"


TESTING = True
DEBUG = True

ENGINE = "dynamo"
ENGINE_OPTIONS = {
    "emulator": "localhost:8000",
    "assert_limits": True
}
