"""Entry points for the Databricks job tasks (see resources/jobs.yml).

Each function parses its own arguments, so the same wheel serves every task:

    collect_status --catalog pulsecity --city london
    ingest_bronze  --catalog pulsecity --city london
    build_silver   --catalog pulsecity --city london
    build_gold     --catalog pulsecity --city london --lookback-days 3
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pulsecity.config import Tables, load_city

log = logging.getLogger("pulsecity")


def _args(argv: list[str] | None, extra=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="pulsecity")
    parser.add_argument("--city", default="london")
    parser.add_argument("--run-id", default="manual")
    if extra:
        extra(parser)
    return parser.parse_args(sys.argv[1:] if argv is None else argv)


def _spark():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def _tfl_app_key(scope: str, key: str) -> str | None:
    """App key from the TFL_APP_KEY env var or a Databricks secret scope.

    TfL works without a key at a lower rate limit, so a missing key is a warning only.
    """
    if os.environ.get("TFL_APP_KEY"):
        return os.environ["TFL_APP_KEY"]
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:  # noqa: BLE001 - any failure here means "no key"
        log.warning("No TfL app key found (%s); using anonymous rate limits", exc)
        return None


def setup_catalog(argv: list[str] | None = None) -> None:
    from pulsecity import pipeline

    args = _args(argv, lambda p: p.add_argument("--storage-root", default=""))
    pipeline.setup_catalog(_spark(), Tables.for_databricks(args.catalog), args.storage_root or None)


def collect_status(argv: list[str] | None = None) -> None:
    from pulsecity.landing import collect_line_status
    from pulsecity.sources.tfl import TflClient

    def extra(p):
        p.add_argument("--secret-scope", default="pulsecity")
        p.add_argument("--secret-key", default="tfl_app_key")
        # Free Edition blocks outbound internet, so the collector runs on GitHub Actions and
        # lands into a local folder that the workflow then uploads to the volume.
        p.add_argument("--landing-root", default="")

    args = _args(argv, extra)
    cfg = load_city(args.city)
    tables = Tables.for_databricks(args.catalog)
    client = TflClient(_tfl_app_key(args.secret_scope, args.secret_key))
    landed = collect_line_status(
        client,
        city=cfg.city,
        source=cfg.source,
        modes=cfg.modes,
        landing_root=args.landing_root or tables.landing_root,
    )
    print(f"landed {landed.path} (HTTP {landed.http_status})")
    if landed.http_status != 200:
        sys.exit(f"TfL returned HTTP {landed.http_status}")


def ingest_bronze(argv: list[str] | None = None) -> None:
    from pulsecity import pipeline

    args = _args(argv)
    spark = _spark()
    tables = Tables.for_databricks(args.catalog)
    pipeline.bronze_stream(spark, tables, load_city(args.city))
    pipeline.log_run(spark, tables, args.run_id, "bronze", {"city": args.city})


def build_silver(argv: list[str] | None = None) -> None:
    from pulsecity import pipeline

    args = _args(argv)
    spark = _spark()
    tables = Tables.for_databricks(args.catalog)
    pipeline.silver_stream(spark, tables)
    pipeline.log_run(spark, tables, args.run_id, "silver", {"city": args.city})


def build_gold(argv: list[str] | None = None) -> None:
    from pulsecity import pipeline

    def extra(p):
        p.add_argument("--lookback-days", type=int, default=3)
        p.add_argument("--full-refresh", action="store_true")

    args = _args(argv, extra)
    spark = _spark()
    tables = Tables.for_databricks(args.catalog)
    metrics = pipeline.build_gold(
        spark, tables, load_city(args.city), args.lookback_days, args.full_refresh
    )
    pipeline.log_run(spark, tables, args.run_id, "gold", metrics)
    print(metrics)
