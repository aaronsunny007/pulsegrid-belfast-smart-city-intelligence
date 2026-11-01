"""Delta Lake write patterns used by the pipeline."""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def overwrite(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def append(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


def overwrite_where(spark: SparkSession, df: DataFrame, table: str, predicate: str | None) -> None:
    """Replace only the rows matching ``predicate`` (or the whole table when it is None)."""
    if predicate is None or not table_exists(spark, table):
        overwrite(df, table)
        return
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)


def merge_upsert(spark: SparkSession, df: DataFrame, table: str, keys: Sequence[str]) -> None:
    """Idempotent upsert: re-running the same batch leaves the table unchanged."""
    if not table_exists(spark, table):
        df.write.format("delta").saveAsTable(table)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def scd2_upsert(
    spark: SparkSession,
    updates: DataFrame,
    table: str,
    keys: Sequence[str],
    tracked: Sequence[str],
    effective_col: str = "effective_at",
) -> None:
    """Slowly Changing Dimension Type 2 in a single MERGE.

    When a tracked attribute changes (e.g. TfL renames a line), the current row is closed
    (``valid_to`` set, ``is_current`` false) and a new current row is inserted, so history
    keeps the name that was in use at the time.

    The trick: changed rows are staged twice. Once with real merge keys (to close the old
    version) and once with NULL merge keys (which never match, so they insert the new version).
    """
    hashed = updates.withColumn("attr_hash", F.sha2(F.concat_ws("||", *tracked), 256))

    if not table_exists(spark, table):
        first = hashed.select(
            *[
                F.expr(expr).alias(col)
                for col, expr in _version_row(keys, tracked, effective_col, "").items()
            ]
        )
        first.write.format("delta").saveAsTable(table)
        return

    current = spark.table(table).where("is_current").select(*keys, "attr_hash")
    changed = (
        hashed.alias("u")
        .join(current.alias("c"), list(keys))
        .where(F.col("u.attr_hash") != F.col("c.attr_hash"))
        .select("u.*")
    )
    mk = [f"_mk_{k}" for k in keys]
    staged = hashed.select(
        "*", *[F.col(k).alias(m) for k, m in zip(keys, mk, strict=True)]
    ).unionByName(changed.select("*", *[F.lit(None).cast("string").alias(m) for m in mk]))

    match = " AND ".join(f"t.{k} = s.{m}" for k, m in zip(keys, mk, strict=True))
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(staged.alias("s"), f"t.is_current AND {match}")
        .whenMatchedUpdate(
            condition="t.attr_hash <> s.attr_hash",
            set={"is_current": "false", "valid_to": f"s.{effective_col}"},
        )
        .whenNotMatchedInsert(values=_version_row(keys, tracked, effective_col, "s."))
        .execute()
    )


def _version_row(
    keys: Sequence[str], tracked: Sequence[str], effective_col: str, prefix: str
) -> dict[str, str]:
    """SQL expressions for a new current version row; ``prefix`` is "s." inside a MERGE."""
    key_list = ", ".join(f"{prefix}{k}" for k in keys)
    return {
        "line_sk": f"substring(sha2(concat_ws('|', {key_list}, "
        f"cast({prefix}{effective_col} as string)), 256), 1, 16)",
        **{c: f"{prefix}{c}" for c in (*keys, *tracked, "attr_hash")},
        "valid_from": f"{prefix}{effective_col}",
        "valid_to": "CAST(NULL AS TIMESTAMP)",
        "is_current": "true",
    }
