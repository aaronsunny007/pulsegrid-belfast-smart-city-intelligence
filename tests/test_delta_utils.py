from datetime import datetime

from pulsecity.delta_utils import merge_upsert, scd2_upsert

LINE_COLS = "city string, line_id string, line_name string, mode string, effective_at timestamp"


def test_merge_upsert_is_idempotent(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS t_merge")
    table = "t_merge.status"
    batch = spark.createDataFrame(
        [("s1", "central", 0, 10), ("s1", "victoria", 0, 9)],
        "snapshot_id string, line_id string, status_seq int, sev int",
    )
    merge_upsert(spark, batch, table, ["snapshot_id", "line_id", "status_seq"])
    merge_upsert(spark, batch, table, ["snapshot_id", "line_id", "status_seq"])
    assert spark.table(table).count() == 2

    fix = spark.createDataFrame([("s1", "victoria", 0, 10)], batch.schema)
    merge_upsert(spark, fix, table, ["snapshot_id", "line_id", "status_seq"])
    assert spark.table(table).where("line_id = 'victoria'").first().sev == 10


def test_scd2_keeps_history_when_a_line_is_renamed(spark):
    """In 2024 TfL split the London Overground into six named lines; history must survive that."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS t_scd")
    table = "t_scd.dim_line"
    keys, tracked = ("city", "line_id"), ("line_name", "mode")
    v1 = spark.createDataFrame(
        [
            ("london", "mildmay", "London Overground", "overground", datetime(2024, 1, 1)),
            ("london", "central", "Central", "tube", datetime(2024, 1, 1)),
        ],
        LINE_COLS,
    )
    scd2_upsert(spark, v1, table, keys, tracked)
    scd2_upsert(spark, v1, table, keys, tracked)  # re-running changes nothing
    assert spark.table(table).count() == 2

    v2 = spark.createDataFrame(
        [
            ("london", "mildmay", "Mildmay", "overground", datetime(2024, 11, 22)),
            ("london", "central", "Central", "tube", datetime(2024, 11, 22)),
        ],
        LINE_COLS,
    )
    scd2_upsert(spark, v2, table, keys, tracked)

    rows = spark.table(table).where("line_id = 'mildmay'").orderBy("valid_from").collect()
    assert [(r.line_name, r.is_current) for r in rows] == [
        ("London Overground", False),
        ("Mildmay", True),
    ]
    assert rows[0].valid_to == datetime(2024, 11, 22)
    assert rows[1].valid_to is None
    assert spark.table(table).where("line_id = 'central'").count() == 1
