"""Silver: parse raw TfL payloads into one typed row per (poll, line, status)."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pulsecity.landing import LINE_STATUS
from pulsecity.quality import LINE_STATUS_RULES, Rule, split_by_rules, to_quarantine
from pulsecity.schemas import TFL_LINE_STATUS_PAYLOAD

# A TfL line can report several statuses at once (e.g. "Part Closure" and "Minor Delays"),
# so status_seq is part of the key.
SILVER_KEYS = ("snapshot_id", "line_id", "status_seq")

SNAPSHOT_RULES = (
    Rule("http_ok", "http_status = 200"),
    Rule("payload_parsed", "lines IS NOT NULL"),
)


@dataclass
class SilverResult:
    valid: DataFrame
    quarantine: DataFrame


def transform_line_status(bronze_df: DataFrame) -> SilverResult:
    parsed = bronze_df.where(F.col("dataset") == LINE_STATUS).withColumn(
        "lines", F.from_json("payload", TFL_LINE_STATUS_PAYLOAD)
    )

    # 1) Whole polls that failed: HTTP errors, unreachable API, unparseable bodies.
    good_polls, bad_polls = split_by_rules(parsed, SNAPSHOT_RULES)
    bad_poll_quarantine = to_quarantine(
        bad_polls.drop("lines", "payload"), stage="silver", dataset=LINE_STATUS
    )

    # 2) Flatten: poll -> line -> status.
    per_line = good_polls.select(
        "snapshot_id",
        "city",
        "source",
        "polled_at",
        "_source_file",
        "_ingested_at",
        F.explode("lines").alias("line"),
    )
    per_status = per_line.select(
        "*", F.posexplode_outer("line.lineStatuses").alias("status_seq", "status")
    )

    # get() returns NULL for an empty array; [0] would fail under ANSI mode (Spark 4, serverless)
    first_period = F.get(F.col("status.validityPeriods"), 0)
    rows = per_status.select(
        "snapshot_id",
        "city",
        "source",
        "polled_at",
        F.col("line.id").alias("line_id"),
        F.col("line.name").alias("line_name"),
        F.col("line.modeName").alias("mode"),
        F.coalesce("status_seq", F.lit(0)).alias("status_seq"),
        F.col("status.statusSeverity").alias("severity_code"),
        F.col("status.statusSeverityDescription").alias("severity_description"),
        F.col("status.reason").alias("reason"),
        F.col("status.disruption.category").alias("disruption_category"),
        F.col("status.disruption.closureText").alias("closure_text"),
        F.to_timestamp(first_period.getField("fromDate")).alias("valid_from"),
        F.to_timestamp(first_period.getField("toDate")).alias("valid_to"),
        "_source_file",
        "_ingested_at",
    ).dropDuplicates(list(SILVER_KEYS))

    # 3) Row-level rules.
    valid, bad_rows = split_by_rules(rows, LINE_STATUS_RULES)
    row_quarantine = to_quarantine(bad_rows, stage="silver", dataset=LINE_STATUS)

    return SilverResult(valid=valid, quarantine=bad_poll_quarantine.unionByName(row_quarantine))
