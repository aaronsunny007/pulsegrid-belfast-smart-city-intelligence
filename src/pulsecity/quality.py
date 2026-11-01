"""Row-level data quality rules. Failing rows go to a quarantine table, not the bin."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    expr: str  # SQL expression that must be TRUE for a valid row (NULL counts as a failure)


LINE_STATUS_RULES = (
    Rule("line_id_present", "line_id IS NOT NULL AND line_id <> ''"),
    Rule("severity_in_range", "severity_code BETWEEN 0 AND 20"),
    Rule("polled_at_present", "polled_at IS NOT NULL"),
    Rule("polled_at_not_future", "polled_at <= current_timestamp() + INTERVAL 10 MINUTES"),
)


def failed_rules_col(rules: tuple[Rule, ...]):
    checks = [
        F.when(~F.coalesce(F.expr(rule.expr), F.lit(False)), F.lit(rule.name)) for rule in rules
    ]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def split_by_rules(df: DataFrame, rules: tuple[Rule, ...]) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows, failing rows with a ``failed_rules`` array)."""
    checked = df.withColumn("failed_rules", failed_rules_col(rules))
    valid = checked.where(F.size("failed_rules") == 0).drop("failed_rules")
    failed = checked.where(F.size("failed_rules") > 0)
    return valid, failed


def to_quarantine(failed: DataFrame, stage: str, dataset: str) -> DataFrame:
    """Normalise failing rows of any shape into the shared quarantine table layout."""
    record_cols = [c for c in failed.columns if c != "failed_rules"]
    return failed.select(
        F.lit(stage).alias("stage"),
        F.lit(dataset).alias("dataset"),
        F.col("snapshot_id"),
        F.col("failed_rules"),
        F.to_json(F.struct(*record_cols)).alias("record_json"),
        F.current_timestamp().alias("quarantined_at"),
    )
