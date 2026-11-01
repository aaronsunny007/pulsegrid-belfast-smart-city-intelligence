"""Explicit schemas. Nothing in the pipeline relies on schema inference."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# The landing envelope written by pulsecity.landing.build_envelope.
LANDING_ENVELOPE = StructType(
    [
        StructField("snapshot_id", StringType()),
        StructField("city", StringType()),
        StructField("source", StringType()),
        StructField("dataset", StringType()),
        StructField("endpoint", StringType()),
        StructField("polled_at", StringType()),
        StructField("http_status", IntegerType()),
        StructField("elapsed_ms", LongType()),
        StructField("error", StringType()),
        StructField("payload", StringType()),
    ]
)

# The parts of the TfL /Line/Mode/{modes}/Status response the pipeline uses.
# Fields not listed here (for example "$type" and "crowding") are ignored.
_TFL_DISRUPTION = StructType(
    [
        StructField("category", StringType()),
        StructField("categoryDescription", StringType()),
        StructField("description", StringType()),
        StructField("closureText", StringType()),
    ]
)

_TFL_VALIDITY_PERIOD = StructType(
    [
        StructField("fromDate", StringType()),
        StructField("toDate", StringType()),
        StructField("isNow", BooleanType()),
    ]
)

_TFL_LINE_STATUS = StructType(
    [
        StructField("statusSeverity", IntegerType()),
        StructField("statusSeverityDescription", StringType()),
        StructField("reason", StringType()),
        StructField("validityPeriods", ArrayType(_TFL_VALIDITY_PERIOD)),
        StructField("disruption", _TFL_DISRUPTION),
    ]
)

_TFL_LINE = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("modeName", StringType()),
        StructField("lineStatuses", ArrayType(_TFL_LINE_STATUS)),
    ]
)

TFL_LINE_STATUS_PAYLOAD = ArrayType(_TFL_LINE)
