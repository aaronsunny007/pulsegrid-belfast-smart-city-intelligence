"""Run the whole pipeline locally on SYNTHETIC data. No Azure or network needed.

    python -m pulsecity.demo            # or: make demo

It writes landing files, then runs Bronze -> Silver -> Gold with local Spark + Delta and
prints the Gold tables. On Databricks the same transforms run on real TfL data.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pulsecity import pipeline
from pulsecity.config import Tables, load_city
from pulsecity.testing.spark import local_spark
from pulsecity.testing.synthetic import generate_landing


def run(workdir: Path, city: str = "london", show: bool = True) -> Tables:
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    cfg = load_city(city)
    tables = Tables(
        catalog=None,
        landing_root=str(workdir / "landing"),
        checkpoint_root=str(workdir / "checkpoints"),
    )
    files = generate_landing(tables.landing_root, city=cfg.city)

    spark = local_spark(str(workdir / "warehouse"))
    pipeline.setup_catalog(spark, tables)
    bronze_rows = pipeline.bronze_batch(spark, tables, cfg)
    silver_metrics = pipeline.silver_batch(spark, tables)
    gold_metrics = pipeline.build_gold(spark, tables, cfg, full_refresh=True)

    if show:
        print(f"\nLanding files written: {files} (synthetic)")
        print(f"Bronze rows: {bronze_rows}")
        print(f"Silver: {silver_metrics}")
        print(f"Gold: {gold_metrics}\n")
        print("== Daily line reliability (worst first) ==")
        spark.table(tables.agg_line_reliability_daily).select(
            "service_date",
            "line_name",
            "reliability_score",
            "risk_level",
            "incidents",
            "unplanned_impact_min",
            "planned_impact_min",
            "coverage_pct",
        ).orderBy("service_date", "reliability_score").show(30, truncate=False)
        print("== Disruption incidents ==")
        spark.table(tables.fact_disruption_incident).select(
            "line_name",
            "started_at",
            "ended_at",
            "duration_min",
            "peak_severity_description",
            "is_ongoing",
        ).orderBy("started_at").show(truncate=False)
        print("== Quarantined records ==")
        spark.table(tables.silver_quarantine).select("stage", "snapshot_id", "failed_rules").show(
            truncate=False
        )
    return tables


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default=".demo")
    parser.add_argument("--city", default="london")
    args = parser.parse_args(argv)
    run(Path(args.workdir).resolve(), args.city)


if __name__ == "__main__":
    main()
