"""Pipeline steps: read tables, call the pure transforms, write tables.

Each step works on Databricks (Unity Catalog names, Auto Loader, streaming checkpoints)
and locally (plain Spark + Delta) so the full flow can be tested end to end.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pulsecity import bronze, gold, silver
from pulsecity.config import CityConfig, Tables
from pulsecity.delta_utils import (
    append,
    merge_upsert,
    overwrite,
    overwrite_where,
    scd2_upsert,
    table_exists,
)
from pulsecity.landing import LINE_STATUS
from pulsecity.reference import severity_df

log = logging.getLogger("pulsecity")

SCHEMAS = ("landing", "bronze", "silver", "gold", "ops")


# --------------------------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------------------------


def setup_catalog(spark: SparkSession, tables: Tables, storage_root: str | None = None) -> None:
    """Create the catalog, schemas and volumes (idempotent).

    With ``storage_root`` (an abfss:// path on ADLS Gen2 covered by a Unity Catalog external
    location), managed tables live under <root>/managed and raw files under <root>/landing,
    so every byte of the lakehouse sits in your own storage account.
    """
    if tables.catalog:
        location = f" MANAGED LOCATION '{storage_root}/managed'" if storage_root else ""
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {tables.catalog}{location}")
    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tables.schema(schema)}")
    if tables.catalog:
        if storage_root:
            spark.sql(
                f"CREATE EXTERNAL VOLUME IF NOT EXISTS {tables.name('landing', 'raw')} "
                f"LOCATION '{storage_root}/landing'"
            )
        else:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {tables.name('landing', 'raw')}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {tables.name('ops', 'checkpoints')}")
    overwrite(severity_df(spark), tables.dim_status_severity)


# --------------------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------------------


def bronze_stream(spark: SparkSession, tables: Tables, cfg: CityConfig) -> None:
    """Auto Loader: pick up only files not seen before, then stop (availableNow)."""
    landing_dir = tables.landing_dir(cfg.city, LINE_STATUS)
    stream = bronze.read_landing_stream(
        spark, landing_dir, tables.checkpoint(f"bronze_{cfg.city}_{LINE_STATUS}_schema")
    )
    (
        bronze.to_bronze(stream)
        .writeStream.option(
            "checkpointLocation", tables.checkpoint(f"bronze_{cfg.city}_{LINE_STATUS}")
        )
        .trigger(availableNow=True)
        .toTable(tables.bronze_line_status)
        .awaitTermination()
    )


def bronze_batch(spark: SparkSession, tables: Tables, cfg: CityConfig) -> int:
    """Local stand-in for bronze_stream (Auto Loader only exists on Databricks)."""
    landing = bronze.read_landing_batch(spark, tables.landing_dir(cfg.city, LINE_STATUS))
    df = bronze.to_bronze(landing)
    if table_exists(spark, tables.bronze_line_status):
        seen = spark.table(tables.bronze_line_status).select("snapshot_id")
        df = df.join(seen, "snapshot_id", "left_anti")
    count = df.count()
    append(df, tables.bronze_line_status)
    return count


# --------------------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------------------


def silver_process_batch(spark: SparkSession, tables: Tables, bronze_batch: DataFrame) -> dict:
    result = silver.transform_line_status(bronze_batch)
    # No .cache(): on Databricks serverless it hit QUOTA_EXCEEDED (50 schemas per catalog)
    # after about a day of hourly runs. The tables are small, so recomputing is cheap.
    valid = result.valid
    quarantine = result.quarantine
    metrics = {
        "bronze_rows": bronze_batch.count(),
        "silver_rows": valid.count(),
        "quarantined_rows": quarantine.count(),
    }
    merge_upsert(spark, valid, tables.silver_line_status, silver.SILVER_KEYS)
    if metrics["quarantined_rows"]:
        append(quarantine, tables.silver_quarantine)
    return metrics


def silver_stream(spark: SparkSession, tables: Tables) -> None:
    """Read only new Bronze rows (streaming checkpoint) and MERGE them into Silver."""

    def _process(batch: DataFrame, batch_id: int) -> None:
        metrics = silver_process_batch(batch.sparkSession, tables, batch)
        log.info("silver batch %s: %s", batch_id, metrics)

    (
        spark.readStream.table(tables.bronze_line_status)
        .writeStream.foreachBatch(_process)
        .option("checkpointLocation", tables.checkpoint("silver_line_status"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


def silver_batch(spark: SparkSession, tables: Tables) -> dict:
    """Local stand-in for silver_stream: reprocess all of Bronze (safe, the MERGE is idempotent)."""
    return silver_process_batch(spark, tables, spark.table(tables.bronze_line_status))


# --------------------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------------------


def gold_start_date(
    spark: SparkSession, tables: Tables, lookback_days: int, full_refresh: bool
) -> date | None:
    """First service date to rebuild, or None for a full rebuild."""
    if full_refresh or not table_exists(spark, tables.fact_line_status):
        return None
    latest = spark.table(tables.fact_line_status).agg(F.max("service_date")).first()[0]
    return None if latest is None else latest - timedelta(days=lookback_days)


def build_gold(
    spark: SparkSession,
    tables: Tables,
    cfg: CityConfig,
    lookback_days: int = 3,
    full_refresh: bool = False,
) -> dict:
    """Rebuild Gold for the last ``lookback_days`` service days (or everything).

    The per-poll fact and the daily/hourly aggregates are rebuilt incrementally with
    replaceWhere. Incidents are rebuilt in full from the fact table: it is small, and a full
    pass is the simplest way to stitch incidents that cross the incremental window correctly.
    """
    start = gold_start_date(spark, tables, lookback_days, full_refresh)
    silver_df = spark.table(tables.silver_line_status).where(F.col("city") == cfg.city)
    severity = spark.table(tables.dim_status_severity)

    if start is not None:
        # One extra day of context so the poll-to-poll durations at the window edge are right.
        silver_df = silver_df.where(F.col("polled_at") >= F.lit(str(start - timedelta(days=1))))
    window_pred = None if start is None else f"city = '{cfg.city}' AND service_date >= '{start}'"

    snapshots = gold.line_snapshots(silver_df, severity, cfg)
    if start is not None:
        snapshots = snapshots.where(F.col("service_date") >= F.lit(start))

    overwrite_where(spark, snapshots, tables.fact_line_status, window_pred)

    all_snapshots = spark.table(tables.fact_line_status).where(F.col("city") == cfg.city)
    incidents = gold.disruption_incidents(all_snapshots)
    overwrite_where(spark, incidents, tables.fact_disruption_incident, f"city = '{cfg.city}'")

    window_incidents = (
        incidents if start is None else incidents.where(F.col("service_date") >= F.lit(start))
    )
    daily = gold.line_reliability_daily(snapshots, window_incidents, cfg)
    overwrite_where(spark, daily, tables.agg_line_reliability_daily, window_pred)
    overwrite_where(
        spark, gold.network_health_hourly(snapshots), tables.agg_network_health_hourly, window_pred
    )
    # Risk is a long-run frequency, so it is rebuilt from all history (a small table).
    overwrite_where(
        spark,
        gold.disruption_risk_hourly(all_snapshots),
        tables.agg_disruption_risk_hourly,
        f"city = '{cfg.city}'",
    )
    overwrite_where(
        spark,
        gold.line_status_current(all_snapshots),
        tables.line_status_current,
        f"city = '{cfg.city}'",
    )
    scd2_upsert(
        spark,
        gold.line_attributes(silver_df),
        tables.dim_line,
        keys=("city", "line_id"),
        tracked=("line_name", "mode"),
    )
    overwrite(gold.date_dimension(all_snapshots), tables.dim_date)
    create_views(spark, tables)

    metrics = {
        "start_date": str(start) if start else "full",
        "snapshot_rows": snapshots.count(),
        "incidents": incidents.count(),
    }
    return metrics


def create_views(spark: SparkSession, tables: Tables) -> None:
    """Reporting views for Power BI and ad-hoc SQL (idempotent)."""
    spark.sql(f"""
        CREATE OR REPLACE VIEW {tables.name("gold", "v_live_alerts")} AS
        SELECT city, line_id, line_name, mode, as_of, polled_at_local,
               worst_severity_description AS status, worst_reason AS reason,
               CASE WHEN worst_is_planned THEN 'Planned works' ELSE 'Live disruption' END
                 AS alert_type
        FROM {tables.line_status_current}
        WHERE NOT is_service_closed AND worst_severity_code NOT IN (10, 18, 19)
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {tables.name("gold", "v_line_reliability_7d")} AS
        WITH latest AS (SELECT max(service_date) AS d FROM {tables.agg_line_reliability_daily})
        SELECT city, line_id, max_by(line_name, service_date) AS line_name,
               round(sum(observed_min - unplanned_impact_min) / sum(observed_min) * 100, 2)
                 AS reliability_score_7d,
               sum(incidents) AS incidents_7d,
               round(avg(mean_time_to_recover_min), 1) AS avg_time_to_recover_min,
               round(sum(planned_impact_min) / 60, 1) AS planned_works_hours_7d,
               round(avg(coverage_pct), 1) AS avg_coverage_pct
        FROM {tables.agg_line_reliability_daily}, latest
        WHERE service_date > latest.d - INTERVAL 7 DAYS
        GROUP BY city, line_id
    """)


# --------------------------------------------------------------------------------------
# Run log
# --------------------------------------------------------------------------------------


def log_run(spark: SparkSession, tables: Tables, run_id: str, step: str, metrics: dict) -> None:
    row = [(run_id, step, json.dumps(metrics, default=str))]
    df = spark.createDataFrame(row, "run_id string, step string, metrics_json string").withColumn(
        "logged_at", F.current_timestamp()
    )
    append(df, tables.run_log)
