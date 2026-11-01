import json
from datetime import datetime, timezone

from pulsecity import bronze, silver
from pulsecity.landing import build_envelope, landing_path, write_atomic
from pulsecity.sources.tfl import ApiResponse
from pulsecity.testing.synthetic import line, status

T0 = datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc)


def land(tmp_path, snapshots):
    """snapshots: list of (snapshot_id, http_status, payload_text)."""
    for snapshot_id, http_status, text in snapshots:
        env = build_envelope(
            snapshot_id=snapshot_id,
            city="london",
            source="tfl",
            dataset="line_status",
            polled_at=T0,
            response=ApiResponse("u", http_status, text, 10),
        )
        write_atomic(landing_path(str(tmp_path), "london", "line_status", T0, snapshot_id), env)


def run_silver(spark, tmp_path, snapshots):
    land(tmp_path, snapshots)
    bronze_df = bronze.to_bronze(bronze.read_landing_batch(spark, str(tmp_path)))
    return silver.transform_line_status(bronze_df)


def test_parses_one_row_per_line_status(spark, tmp_path):
    payload = [
        line("central", "Central", "tube", [status(10, "central")]),
        line(
            "jubilee",
            "Jubilee",
            "tube",
            [
                status(5, "jubilee", "JUBILEE LINE: planned works", "PlannedWork"),
                status(9, "jubilee", "JUBILEE LINE: minor delays", "RealTime"),
            ],
        ),
    ]
    result = run_silver(spark, tmp_path, [("s1", 200, json.dumps(payload))])
    rows = {(r.line_id, r.status_seq): r for r in result.valid.collect()}

    assert set(rows) == {("central", 0), ("jubilee", 0), ("jubilee", 1)}
    assert rows[("jubilee", 0)].disruption_category == "PlannedWork"
    assert rows[("jubilee", 1)].severity_description == "Minor Delays"
    assert rows[("central", 0)].polled_at is not None
    assert rows[("central", 0)]._source_file.endswith("s1.json")
    assert result.quarantine.count() == 0


def test_failed_and_unparseable_polls_are_quarantined(spark, tmp_path):
    result = run_silver(
        spark,
        tmp_path,
        [
            ("bad_http", 503, '{"message":"Service Unavailable"}'),
            ("bad_json", 200, "<html>gateway error</html>"),
        ],
    )
    assert result.valid.count() == 0
    failures = {r.snapshot_id: r.failed_rules for r in result.quarantine.collect()}
    assert failures == {"bad_http": ["http_ok"], "bad_json": ["payload_parsed"]}


def test_bad_rows_are_quarantined_and_good_rows_kept(spark, tmp_path):
    payload = [
        line("central", "Central", "tube", [status(10, "central")]),
        line("", "Mystery", "tube", [status(10, "")]),  # missing id
        line("circle", "Circle", "tube", []),  # no status at all
    ]
    payload[0]["lineStatuses"].append({"statusSeverity": 42, "statusSeverityDescription": "?"})
    result = run_silver(spark, tmp_path, [("s1", 200, json.dumps(payload))])

    assert [(r.line_id, r.severity_code) for r in result.valid.collect()] == [("central", 10)]
    failed = sorted(tuple(r.failed_rules) for r in result.quarantine.collect())
    assert failed == [("line_id_present",), ("severity_in_range",), ("severity_in_range",)]
