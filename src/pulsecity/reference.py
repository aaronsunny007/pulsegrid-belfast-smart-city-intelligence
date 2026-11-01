"""Reference data: TfL status severities and how much each one hurts service.

The codes and descriptions are TfL's (see /Line/Meta/Severity). The impact weights and
the "planned by default" flags are this project's modelling choices, explained in
docs/reliability_methodology.md. Change them here and every Gold table follows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

SERVICE_CLOSED = 20  # line is shut for the night; excluded from reliability

# (code, TfL description, impact weight 0-1, planned by default, category)
SEVERITIES: tuple[tuple[int, str, float, bool, str], ...] = (
    (0, "Special Service", 0.25, False, "Degraded"),
    (1, "Closed", 1.00, False, "Not running"),
    (2, "Suspended", 1.00, False, "Not running"),
    (3, "Part Suspended", 0.60, False, "Partly running"),
    (4, "Planned Closure", 1.00, True, "Not running"),
    (5, "Part Closure", 0.50, True, "Partly running"),
    (6, "Severe Delays", 0.60, False, "Delayed"),
    (7, "Reduced Service", 0.40, False, "Degraded"),
    (8, "Bus Service", 0.75, True, "Not running"),
    (9, "Minor Delays", 0.25, False, "Delayed"),
    (10, "Good Service", 0.00, False, "Good"),
    (11, "Part Closed", 0.50, True, "Partly running"),
    (12, "Exit Only", 0.25, False, "Degraded"),
    (13, "No Step Free Access", 0.10, False, "Degraded"),
    (14, "Change of frequency", 0.20, True, "Degraded"),
    (15, "Diverted", 0.40, False, "Degraded"),
    (16, "Not Running", 1.00, False, "Not running"),
    (17, "Issues Reported", 0.25, False, "Degraded"),
    (18, "No Issues", 0.00, False, "Good"),
    (19, "Information", 0.00, False, "Good"),
    (SERVICE_CLOSED, "Service Closed", 0.00, True, "Closed for the night"),
)

SEVERITY_SCHEMA = StructType(
    [
        StructField("severity_code", IntegerType(), False),
        StructField("severity_description", StringType(), False),
        StructField("impact_weight", DoubleType(), False),
        StructField("planned_by_default", BooleanType(), False),
        StructField("severity_category", StringType(), False),
    ]
)


def severity_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(list(SEVERITIES), SEVERITY_SCHEMA)
