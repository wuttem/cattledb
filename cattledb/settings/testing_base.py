from cattledb.settings.default import *

TESTING = True
DEBUG = True

METRICS = [
    MetricDefinition("hum", "hum", MetricType.FLOATSERIES, True),
    MetricDefinition("ph", "ph", MetricType.FLOATSERIES, True),
    MetricDefinition("act", "act", MetricType.FLOATSERIES, True),
    MetricDefinition("temp", "tmp", MetricType.FLOATSERIES, True),
]

EVENTS = [
    EventDefinition("test_daily", EventSeriesType.DAILY),
    EventDefinition("test_monthly", EventSeriesType.MONTHLY),
    EventDefinition("test_monthly_*", EventSeriesType.MONTHLY)
]