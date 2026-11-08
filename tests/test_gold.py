import dataclasses
from datetime import date, datetime

import pytest

from pulsecity import gold
from pulsecity.reference import severity_df

SILVER_COLS = (
    "snapshot_id string, city string, polled_at timestamp, line_id string, line_name string, "
    "mode string, severity_code int, severity_description string, reason string, "
    "disruption_category string"
)
DESC = {
    6: "Severe Delays",
    9: "Minor Delays",
    10: "Good Service",
    5: "Part Closure",
    20: "Service Closed",
}


def silver(spark, rows):
    """rows: (utc 'YYYY-MM-DD HH:MM', line_id, severity, category or None)."""
    data = [
        (
            f"s-{ts}",
            "london",
            datetime.fromisoformat(ts),
            line_id,
            line_id.title(),
            "tube",
            sev,
            DESC[sev],
            None if sev == 10 else f"{line_id} issue",
            cat,
        )
        for ts, line_id, sev, cat in rows
    ]
    return spark.createDataFrame(data, SILVER_COLS)


def snaps(spark, london, rows):
    return gold.line_snapshots(silver(spark, rows), severity_df(spark), london)


def by_time(df):
    return {r.polled_at.strftime("%H:%M"): r for r in df.collect()}


def test_duration_is_time_to_next_poll_and_gaps_are_not_counted(spark, london):
    rows = [(f"2026-10-16 {t}", "central", 10, None) for t in ("10:00", "10:05", "10:30", "10:35")]
    s = by_time(snaps(spark, london, rows))
    assert [s[t].duration_min for t in ("10:00", "10:05", "10:30", "10:35")] == [5, 5, 5, 5]
    assert s["10:05"].data_gap_after and not s["10:00"].data_gap_after


@pytest.mark.parametrize(
    ("utc", "service_date"),
    [
        ("2026-10-16 23:30", date(2026, 10, 16)),  # 00:30 BST on the 17th -> still Friday's service
        ("2026-10-17 03:30", date(2026, 10, 17)),  # 04:30 BST -> new service day
        ("2026-12-01 03:30", date(2026, 11, 30)),  # 03:30 GMT (winter) -> previous service day
        ("2026-12-01 04:30", date(2026, 12, 1)),
    ],
)
def test_service_date_uses_local_time_across_bst_and_gmt(spark, london, utc, service_date):
    (row,) = snaps(spark, london, [(utc, "central", 10, None)]).collect()
    assert row.service_date == service_date


def test_planned_works_are_separated_from_unplanned_disruption(spark, london):
    rows = [
        ("2026-10-17 10:00", "jubilee", 5, "PlannedWork"),
        ("2026-10-17 10:00", "jubilee", 9, "RealTime"),
    ]
    (row,) = snaps(spark, london, rows).collect()
    assert row.planned_impact == 0.5
    assert row.unplanned_impact == 0.25
    assert row.is_disrupted
    assert row.worst_severity_description == "Part Closure"
    assert row.status_count == 2


def test_incidents_are_sessionised_and_split_by_data_gaps(spark, london):
    timeline = [
        ("10:00", 10),
        ("10:05", 9),
        ("10:10", 9),
        ("10:15", 10),
        ("10:20", 6),
        ("11:00", 6),
        ("11:05", 6),
    ]  # 10:20 -> 11:00 is a data gap
    rows = [
        (f"2026-10-16 {t}", "central", sev, "RealTime" if sev != 10 else None)
        for t, sev in timeline
    ]
    incidents = sorted(
        gold.disruption_incidents(snaps(spark, london, rows)).collect(), key=lambda r: r.started_at
    )

    assert [
        (i.started_at.strftime("%H:%M"), i.duration_min, i.polls, i.is_ongoing) for i in incidents
    ] == [("10:05", 10.0, 2, False), ("10:20", 5.0, 1, False), ("11:00", 10.0, 2, True)]
    assert incidents[0].ended_at == datetime(2026, 10, 16, 10, 15)
    assert incidents[0].peak_severity_description == "Minor Delays"
    assert incidents[2].ended_at is None


def test_daily_score_and_risk_level(spark, london):
    cfg = dataclasses.replace(london, min_observed_minutes=30)
    times = [f"2026-10-16 10:{m:02d}" for m in range(0, 60, 5)]  # 12 polls = 60 minutes
    rows = [
        (t, "central", 6 if i < 2 else 10, "RealTime" if i < 2 else None)
        for i, t in enumerate(times)
    ]
    rows += [("2026-10-16 23:30", "central", 20, None)]  # night closure: not counted
    snapshots = snaps(spark, cfg, rows)
    daily = gold.line_reliability_daily(snapshots, gold.disruption_incidents(snapshots), cfg)
    (row,) = daily.collect()

    assert row.observed_min == 60.0
    assert row.unplanned_impact_min == 6.0  # 2 polls * 5 min * 0.6
    assert row.reliability_score == 90.0  # 100 * (1 - 6/60)
    assert row.good_service_pct == pytest.approx(83.33)
    assert row.incidents == 1 and row.mean_time_to_recover_min == 10.0
    assert row.risk_level == "High Risk"


def test_too_little_data_gets_no_risk_label(spark, london):
    snapshots = snaps(spark, london, [("2026-10-16 10:00", "central", 10, None)])
    (row,) = gold.line_reliability_daily(
        snapshots, gold.disruption_incidents(snapshots), london
    ).collect()
    assert row.risk_level == "Insufficient data"


def test_disruption_risk_is_smoothed_and_labelled_by_confidence(spark, london):
    def hour(day, start_hour, disrupted):
        return [
            (
                f"2026-10-{day} {start_hour:02d}:{m:02d}",
                "central",
                6 if disrupted and m < 10 else 10,
                "RealTime" if disrupted and m < 10 else None,
            )
            for m in range(0, 60, 5)
        ]

    # Fridays in BST: 09:xx UTC is local hour 10, 10:xx UTC is local hour 11.
    rows = hour(16, 9, True) + hour(16, 10, False) + hour(23, 9, False)
    risk = {
        r.hour_of_day: r for r in gold.disruption_risk_hourly(snaps(spark, london, rows)).collect()
    }

    ten = risk[10]
    assert ten.day_type == "weekday" and ten.days_observed == 2 and ten.days_disrupted == 1
    assert ten.risk_pct == pytest.approx(8.33)  # 10 of 120 minutes
    # line average is 10/180; (10 + 120 * 10/180) / (120 + 120)
    assert ten.smoothed_risk_pct == pytest.approx(6.94)
    assert ten.confidence == "Low"
    assert risk[11].risk_pct == 0.0


def test_disruption_risk_separates_weekends(spark, london):
    rows = [("2026-10-17 09:00", "central", 6, "RealTime")]  # Saturday
    (row,) = gold.disruption_risk_hourly(snaps(spark, london, rows)).collect()
    assert row.day_type == "weekend" and row.risk_pct == 100.0


def test_network_health_is_time_weighted(spark, london):
    rows = [
        ("2026-10-16 10:00", "central", 6, "RealTime"),
        ("2026-10-16 10:00", "victoria", 10, None),
    ]
    (row,) = gold.network_health_hourly(snaps(spark, london, rows)).collect()
    assert row.lines_observed == 2 and row.lines_disrupted == 1
    assert row.network_health_index == 70.0  # 100 * (1 - (0.6 + 0) / 2)
