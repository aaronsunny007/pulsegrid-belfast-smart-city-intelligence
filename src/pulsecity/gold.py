"""Gold: turn status polls into facts, incidents and reliability scores.

Every function here is a pure DataFrame -> DataFrame transform, so it can be unit
tested locally. Reading and writing tables happens in pulsecity.jobs.

Spark's session time zone must be UTC (the jobs set it), so timestamps are UTC
instants and local London time is derived explicitly with from_utc_timestamp.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pulsecity.config import CityConfig
from pulsecity.reference import SERVICE_CLOSED


def _minutes_between(start, end):
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 60.0


# --------------------------------------------------------------------------------------
# fact_line_status: one row per line per poll
# --------------------------------------------------------------------------------------


def line_snapshots(silver: DataFrame, severity: DataFrame, cfg: CityConfig) -> DataFrame:
    """Collapse the statuses a line reports in one poll into a single row.

    - ``unplanned_impact``: worst impact among unplanned statuses (0 = good, 1 = not running)
    - ``planned_impact``: worst impact among planned works
    - ``duration_min``: how long this poll is taken to represent (time to the next poll,
      capped: a gap longer than ``max_gap_minutes`` counts as missing data instead)
    """
    sev = severity.select("severity_code", "impact_weight", "planned_by_default")
    statuses = silver.join(F.broadcast(sev), "severity_code", "left").withColumn(
        "is_planned",
        F.when(F.col("disruption_category") == "PlannedWork", True)
        .when(F.col("disruption_category") == "RealTime", False)
        .otherwise(F.coalesce("planned_by_default", F.lit(False))),
    )
    impact = F.coalesce("impact_weight", F.lit(0.0))

    per_poll = statuses.groupBy("snapshot_id", "city", "polled_at", "line_id").agg(
        F.first("line_name").alias("line_name"),
        F.first("mode").alias("mode"),
        F.count("*").alias("status_count"),
        F.coalesce(F.max(F.when(~F.col("is_planned"), impact)), F.lit(0.0)).alias(
            "unplanned_impact"
        ),
        F.coalesce(F.max(F.when(F.col("is_planned"), impact)), F.lit(0.0)).alias("planned_impact"),
        F.bool_or(F.col("severity_code") == SERVICE_CLOSED).alias("is_service_closed"),
        F.max_by(
            F.struct(
                F.col("severity_code").alias("code"),
                F.col("severity_description").alias("description"),
                F.col("reason").alias("reason"),
                F.col("is_planned").alias("is_planned"),
            ),
            # worst impact wins; ties broken towards the lower (more severe) TfL code
            impact * 100 - F.col("severity_code") / 100.0,
        ).alias("worst"),
    )

    by_line = Window.partitionBy("city", "line_id").orderBy("polled_at")
    gap = _minutes_between(F.col("polled_at"), F.lead("polled_at").over(by_line))
    local_ts = F.from_utc_timestamp("polled_at", cfg.timezone)

    return (
        per_poll.withColumn("gap_to_next_min", gap)
        .withColumn("polled_at_local", local_ts)
        .select(
            "snapshot_id",
            "city",
            "line_id",
            "line_name",
            "mode",
            "polled_at",
            "polled_at_local",
            F.to_date(
                F.col("polled_at_local") - F.expr(f"INTERVAL {cfg.service_day_start_hour} HOURS")
            ).alias("service_date"),
            F.date_trunc("hour", "polled_at_local").alias("local_hour"),
            F.col("worst.code").alias("worst_severity_code"),
            F.col("worst.description").alias("worst_severity_description"),
            F.col("worst.reason").alias("worst_reason"),
            F.col("worst.is_planned").alias("worst_is_planned"),
            "unplanned_impact",
            "planned_impact",
            ((F.col("unplanned_impact") > 0) & ~F.col("is_service_closed")).alias("is_disrupted"),
            "is_service_closed",
            "status_count",
            F.when(F.col("gap_to_next_min").isNull(), float(cfg.poll_interval_minutes))
            .when(F.col("gap_to_next_min") <= cfg.max_gap_minutes, F.col("gap_to_next_min"))
            .otherwise(float(cfg.poll_interval_minutes))
            .alias("duration_min"),
            F.coalesce(F.col("gap_to_next_min") > cfg.max_gap_minutes, F.lit(False)).alias(
                "data_gap_after"
            ),
        )
    )


# --------------------------------------------------------------------------------------
# fact_disruption_incident: consecutive disrupted polls joined into one incident
# --------------------------------------------------------------------------------------


def disruption_incidents(snapshots: DataFrame) -> DataFrame:
    """Sessionise polls into incidents.

    A new episode starts whenever a line's disrupted/not-disrupted state changes, or after a
    data gap (we cannot claim an incident continued through time we did not observe).
    """
    by_line = Window.partitionBy("city", "line_id").orderBy("polled_at")
    running = by_line.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    prev_state = F.lag("is_disrupted").over(by_line)
    prev_gap = F.lag("data_gap_after").over(by_line)
    starts_episode = prev_state.isNull() | (prev_state != F.col("is_disrupted")) | prev_gap

    episodes = (
        snapshots.withColumn("starts_episode", starts_episode.cast("int"))
        .withColumn("episode_no", F.sum("starts_episode").over(running))
        .withColumn(
            "line_last_polled", F.max("polled_at").over(Window.partitionBy("city", "line_id"))
        )
    )

    incidents = (
        episodes.where("is_disrupted")
        .groupBy("city", "line_id", "episode_no")
        .agg(
            F.max_by("line_name", "polled_at").alias("line_name"),
            F.max_by("mode", "polled_at").alias("mode"),
            F.min("polled_at").alias("started_at"),
            F.max("polled_at").alias("last_seen_at"),
            F.min_by("service_date", "polled_at").alias("service_date"),
            F.sum("duration_min").alias("duration_min"),
            F.count("*").alias("polls"),
            F.max("unplanned_impact").alias("peak_impact"),
            F.max_by(
                F.struct(
                    F.col("worst_severity_code").alias("code"),
                    F.col("worst_severity_description").alias("description"),
                ),
                F.col("unplanned_impact"),
            ).alias("peak"),
            F.min_by("worst_reason", "polled_at").alias("first_reason"),
            F.max("line_last_polled").alias("line_last_polled"),
        )
    )

    is_ongoing = F.col("last_seen_at") == F.col("line_last_polled")
    return incidents.select(
        F.substring(F.sha2(F.concat_ws("|", "line_id", "started_at"), 256), 1, 16).alias(
            "incident_id"
        ),
        "city",
        "line_id",
        "line_name",
        "mode",
        "service_date",
        "started_at",
        F.when(
            ~is_ongoing,
            F.timestamp_seconds(
                F.unix_timestamp("started_at") + F.round(F.col("duration_min") * 60).cast("long")
            ),
        ).alias("ended_at"),
        F.round("duration_min", 1).alias("duration_min"),
        "polls",
        F.col("peak.code").alias("peak_severity_code"),
        F.col("peak.description").alias("peak_severity_description"),
        "peak_impact",
        "first_reason",
        is_ongoing.alias("is_ongoing"),
    )


# --------------------------------------------------------------------------------------
# agg_line_reliability_daily: the score
# --------------------------------------------------------------------------------------


def line_reliability_daily(
    snapshots: DataFrame, incidents: DataFrame, cfg: CityConfig
) -> DataFrame:
    """Daily reliability per line.

    reliability_score = 100 * (1 - unplanned impact-minutes / observed minutes)

    Planned engineering works are reported separately (planned_impact_min) and do not
    lower the score: the score measures how well the line ran against its plan.
    """
    operating = snapshots.where(~F.col("is_service_closed"))
    dur = F.col("duration_min")

    daily = operating.groupBy("service_date", "city", "line_id").agg(
        F.max_by("line_name", "polled_at").alias("line_name"),
        F.max_by("mode", "polled_at").alias("mode"),
        F.count("*").alias("polls"),
        F.min("polled_at").alias("first_poll_at"),
        F.max("polled_at").alias("last_poll_at"),
        F.sum(dur).alias("observed_min"),
        F.sum(
            F.when(
                (F.col("unplanned_impact") == 0) & (F.col("planned_impact") == 0), dur
            ).otherwise(0.0)
        ).alias("good_service_min"),
        F.sum(F.when(F.col("is_disrupted"), dur).otherwise(0.0)).alias("disrupted_min"),
        F.sum(dur * F.col("unplanned_impact")).alias("unplanned_impact_min"),
        F.sum(dur * F.col("planned_impact")).alias("planned_impact_min"),
    )

    incident_stats = incidents.groupBy("service_date", "city", "line_id").agg(
        F.count("*").alias("incidents"),
        F.avg(F.when(~F.col("is_ongoing"), F.col("duration_min"))).alias(
            "mean_time_to_recover_min"
        ),
        F.max("duration_min").alias("longest_incident_min"),
    )

    span = _minutes_between(F.col("first_poll_at"), F.col("last_poll_at")) + float(
        cfg.poll_interval_minutes
    )
    score = F.round(100.0 * (1.0 - F.col("unplanned_impact_min") / F.col("observed_min")), 2)
    bands = cfg.risk_bands

    return (
        daily.join(incident_stats, ["service_date", "city", "line_id"], "left")
        .withColumn("reliability_score", score)
        .select(
            "service_date",
            "city",
            "line_id",
            "line_name",
            "mode",
            "polls",
            F.round("observed_min", 1).alias("observed_min"),
            F.round(F.least(F.lit(100.0), 100.0 * F.col("observed_min") / span), 1).alias(
                "coverage_pct"
            ),
            F.round("good_service_min", 1).alias("good_service_min"),
            F.round(100.0 * F.col("good_service_min") / F.col("observed_min"), 2).alias(
                "good_service_pct"
            ),
            F.round("disrupted_min", 1).alias("disrupted_min"),
            F.round("unplanned_impact_min", 1).alias("unplanned_impact_min"),
            F.round("planned_impact_min", 1).alias("planned_impact_min"),
            F.coalesce("incidents", F.lit(0)).alias("incidents"),
            F.round("mean_time_to_recover_min", 1).alias("mean_time_to_recover_min"),
            F.round("longest_incident_min", 1).alias("longest_incident_min"),
            "reliability_score",
            F.when(F.col("observed_min") < cfg.min_observed_minutes, "Insufficient data")
            .when(F.col("reliability_score") >= bands.stable_min, "Stable")
            .when(F.col("reliability_score") >= bands.moderate_min, "Moderate Risk")
            .otherwise("High Risk")
            .alias("risk_level"),
        )
    )


# --------------------------------------------------------------------------------------
# agg_network_health_hourly and line_status_current
# --------------------------------------------------------------------------------------


def network_health_hourly(snapshots: DataFrame) -> DataFrame:
    operating = snapshots.where(~F.col("is_service_closed"))
    dur = F.col("duration_min")
    return operating.groupBy("city", "service_date", "local_hour").agg(
        F.countDistinct("line_id").alias("lines_observed"),
        F.countDistinct(F.when(F.col("is_disrupted"), F.col("line_id"))).alias("lines_disrupted"),
        F.round(F.sum(dur), 1).alias("line_minutes_observed"),
        F.round(100.0 * (1.0 - F.sum(dur * F.col("unplanned_impact")) / F.sum(dur)), 2).alias(
            "network_health_index"
        ),
        F.round(100.0 * F.sum(dur * F.col("planned_impact")) / F.sum(dur), 2).alias(
            "planned_works_pct"
        ),
    )


# Weight of the line's own average, in minutes, when smoothing thin hourly samples.
RISK_PRIOR_MINUTES = 120.0


def disruption_risk_hourly(snapshots: DataFrame) -> DataFrame:
    """How often each line has been disrupted at each local hour, weekday vs weekend.

    This is a historical frequency, not a forecast model. ``smoothed_risk_pct`` pulls a thin
    hourly sample towards the line's overall disruption rate so a few bad polls cannot
    claim a 100% risk; ``confidence`` says how many distinct days back the number.
    """
    operating = snapshots.where(~F.col("is_service_closed"))
    dur = F.col("duration_min")
    is_weekend = F.dayofweek("service_date").isin(1, 7)

    hourly = (
        operating.withColumn("day_type", F.when(is_weekend, "weekend").otherwise("weekday"))
        .withColumn("hour_of_day", F.hour("local_hour"))
        .groupBy("city", "line_id", "day_type", "hour_of_day")
        .agg(
            F.max_by("line_name", "polled_at").alias("line_name"),
            F.max_by("mode", "polled_at").alias("mode"),
            F.sum(dur).alias("observed_min"),
            F.sum(F.when(F.col("is_disrupted"), dur).otherwise(0.0)).alias("disrupted_min"),
            F.countDistinct("service_date").alias("days_observed"),
            F.countDistinct(F.when(F.col("is_disrupted"), F.col("service_date"))).alias(
                "days_disrupted"
            ),
        )
    )

    line = Window.partitionBy("city", "line_id")
    baseline = F.sum("disrupted_min").over(line) / F.sum("observed_min").over(line)
    smoothed = (F.col("disrupted_min") + RISK_PRIOR_MINUTES * baseline) / (
        F.col("observed_min") + RISK_PRIOR_MINUTES
    )

    return hourly.select(
        "city",
        "line_id",
        "line_name",
        "mode",
        "day_type",
        "hour_of_day",
        F.round("observed_min", 1).alias("observed_min"),
        F.round("disrupted_min", 1).alias("disrupted_min"),
        "days_observed",
        "days_disrupted",
        F.round(100.0 * F.col("disrupted_min") / F.col("observed_min"), 2).alias("risk_pct"),
        F.round(100.0 * smoothed, 2).alias("smoothed_risk_pct"),
        F.when(F.col("days_observed") < 7, "Low")
        .when(F.col("days_observed") < 21, "Medium")
        .otherwise("Good")
        .alias("confidence"),
    )


def line_status_current(snapshots: DataFrame) -> DataFrame:
    latest = Window.partitionBy("city", "line_id").orderBy(F.col("polled_at").desc())
    return (
        snapshots.withColumn("rn", F.row_number().over(latest))
        .where("rn = 1")
        .select(
            "city",
            "line_id",
            "line_name",
            "mode",
            F.col("polled_at").alias("as_of"),
            "polled_at_local",
            "worst_severity_code",
            "worst_severity_description",
            "worst_reason",
            "worst_is_planned",
            "unplanned_impact",
            "is_disrupted",
            "is_service_closed",
        )
    )


# --------------------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------------------


def line_attributes(silver: DataFrame) -> DataFrame:
    """Latest attributes per line, with the first time that combination was seen.

    This is the input to the dim_line SCD Type 2 merge.
    """
    combos = silver.groupBy("city", "line_id", "line_name", "mode").agg(
        F.min("polled_at").alias("effective_at"), F.max("polled_at").alias("last_seen_at")
    )
    latest = Window.partitionBy("city", "line_id").orderBy(F.col("last_seen_at").desc())
    return (
        combos.withColumn("rn", F.row_number().over(latest))
        .where("rn = 1")
        .select("city", "line_id", "line_name", "mode", "effective_at")
    )


def date_dimension(snapshots: DataFrame) -> DataFrame:
    bounds = snapshots.agg(F.min("service_date").alias("start"), F.max("service_date").alias("end"))
    days = bounds.select(F.explode(F.sequence("start", F.date_add("end", 7))).alias("date"))
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_trunc("week", "date").cast("date").alias("week_start"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
