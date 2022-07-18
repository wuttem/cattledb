from cattledb.settings.default import *
from cattledb.core.models import MetricDefinition, MetricType

TABLE_PREFIX = "weatherdata"
READ_ONLY = False
ADMIN = True
ENGINE = "dynamo"
ENGINE_OPTIONS = {
    "assert_limits": True,
    "region": "eu-central-1",
    "access_key_id": "default"
}

METRICS = [
     MetricDefinition("temperature", "temp", MetricType.FLOATSERIES, True),
     MetricDefinition("humidity", "hum", MetricType.FLOATSERIES, True),
     MetricDefinition("rain", "rain", MetricType.FLOATSERIES, True),
     MetricDefinition("wind_dir", "wdir", MetricType.FLOATSERIES, True),
     MetricDefinition("wind", "wind", MetricType.FLOATSERIES, True),
     MetricDefinition("light", "light", MetricType.FLOATSERIES, True),
     MetricDefinition("battery", "bat", MetricType.FLOATSERIES, True)
]