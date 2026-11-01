"""End to end: synthetic landing files -> Bronze -> Silver -> Gold with local Spark + Delta."""

from datetime import date

import pytest

from pulsecity import demo, pipeline
from pulsecity.config import load_city


@pytest.fixture(scope="module")
def tables(spark, tmp_path_factory):
    return demo.run(tmp_path_factory.mktemp("e2e"), show=False)


def daily(spark, tables, line_id, day):
    return (
        spark.table(tables.agg_line_reliability_daily)
        .where(f"line_id = '{line_id}' AND service_date = '{day}'")
        .first()
    )


def test_scores_match_the_scenario(spark, tables):
    central = daily(spark, tables, "central", date(2026, 10, 16))
    # 45 min severe (0.6) + 25 min minor (0.25) = 33.25 impact-minutes
    assert central.unplanned_impact_min == pytest.approx(33.3, abs=0.1)
    assert central.risk_level == "Moderate Risk"
    assert central.incidents == 1  # severe then minor delays is one incident, not two


def test_planned_works_do_not_lower_the_score(spark, tables):
    jubilee = daily(spark, tables, "jubilee", date(2026, 10, 17))
    assert jubilee.reliability_score == 100.0
    assert jubilee.planned_impact_min > 500


def test_collector_outage_lowers_coverage_not_reliability(spark, tables):
    bakerloo = daily(spark, tables, "bakerloo", date(2026, 10, 16))
    assert bakerloo.reliability_score == 100.0
    assert bakerloo.coverage_pct < 100.0


def test_failed_poll_is_quarantined(spark, tables):
    rules = [r.failed_rules for r in spark.table(tables.silver_quarantine).collect()]
    assert rules == [["http_ok"]]


def test_incident_count_and_current_status(spark, tables):
    assert spark.table(tables.fact_disruption_incident).count() == 8
    assert spark.table(tables.line_status_current).count() == 13
    assert spark.table(tables.dim_line).where("is_current").count() == 13


def test_rerunning_is_idempotent(spark, tables):
    cfg = load_city("london")
    before = {
        t: spark.table(t).count()
        for t in (
            tables.silver_line_status,
            tables.fact_line_status,
            tables.fact_disruption_incident,
            tables.agg_line_reliability_daily,
            tables.dim_line,
        )
    }

    assert pipeline.bronze_batch(spark, tables, cfg) == 0  # no new files -> nothing ingested
    pipeline.silver_batch(spark, tables)
    metrics = pipeline.build_gold(spark, tables, cfg, lookback_days=1)  # incremental path

    assert metrics["start_date"] == "2026-10-16"
    after = {t: spark.table(t).count() for t in before}
    assert after == before
