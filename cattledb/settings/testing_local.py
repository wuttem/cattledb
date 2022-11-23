from cattledb.settings.testing_base import *

TESTING = True
DEBUG = True

ENGINE = "localsql"
# ENGINE_OPTIONS = {
#     "data_dir": ":memory:",
#     "assert_limits": True,
#     "in_memory": True
# }
ENGINE_OPTIONS = {
    "data_dir": ".",
    "assert_limits": True
}