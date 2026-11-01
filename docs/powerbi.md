# Power BI report

The report reads the Gold layer through a Databricks SQL warehouse. All business logic lives in
Gold and in the explicit DAX measures below. No visual uses Power BI's default "Sum of" or
"Count of" aggregations, which caused the wrong numbers in the v1 report (see `archive/v1`).

## Tables and storage mode

| Table | Mode | Why |
|---|---|---|
| `gold.dim_line` (filter `is_current = TRUE` in Power Query) | Import | small dimension |
| `gold.dim_date` | Import | mark as date table |
| `gold.agg_line_reliability_daily` | Import | main fact, refreshed hourly |
| `gold.agg_network_health_hourly` | Import | trend visuals |
| `gold.fact_disruption_incident` | Import | incident list and MTTR |
| `gold.v_live_alerts` | DirectQuery | always current, no refresh needed |

This makes a composite model: fast imported history plus a live alert feed.

## Relationships

```
dim_line[line_id]  1 ──* agg_line_reliability_daily[line_id]
dim_line[line_id]  1 ──* fact_disruption_incident[line_id]
dim_date[date]     1 ──* agg_line_reliability_daily[service_date]
dim_date[date]     1 ──* fact_disruption_incident[service_date]
dim_date[date]     1 ──* agg_network_health_hourly[service_date]
```

All single direction, dimension to fact.

## Measures

Put these in a dedicated `_Measures` table.

```DAX
Observed Minutes = SUM ( agg_line_reliability_daily[observed_min] )

Unplanned Impact Minutes = SUM ( agg_line_reliability_daily[unplanned_impact_min] )

Reliability Score =
DIVIDE ( [Observed Minutes] - [Unplanned Impact Minutes], [Observed Minutes] ) * 100

Good Service % =
DIVIDE ( SUM ( agg_line_reliability_daily[good_service_min] ), [Observed Minutes] ) * 100

Planned Works Hours = SUM ( agg_line_reliability_daily[planned_impact_min] ) / 60

Incidents = COUNTROWS ( fact_disruption_incident )

Mean Time To Recover (min) =
AVERAGEX (
    FILTER ( fact_disruption_incident, NOT fact_disruption_incident[is_ongoing] ),
    fact_disruption_incident[duration_min]
)

Risk Level =
VAR s = [Reliability Score]
RETURN
    SWITCH (
        TRUE (),
        [Observed Minutes] < 120, "Insufficient data",
        s >= 98, "Stable",
        s >= 95, "Moderate Risk",
        "High Risk"
    )

Reliability Change vs Previous 7 Days =
VAR cur = [Reliability Score]
VAR prev =
    CALCULATE ( [Reliability Score], DATEADD ( dim_date[date], -7, DAY ) )
RETURN
    IF ( NOT ISBLANK ( prev ), cur - prev )

Network Health Index =
DIVIDE (
    SUMX (
        agg_network_health_hourly,
        agg_network_health_hourly[network_health_index]
            * agg_network_health_hourly[line_minutes_observed]
    ),
    SUM ( agg_network_health_hourly[line_minutes_observed] )
)

Live Alerts = COUNTROWS ( v_live_alerts )

Data Coverage % = AVERAGE ( agg_line_reliability_daily[coverage_pct] )
```

`Reliability Score` is time-weighted: it divides total impact by total observed time, instead of
averaging daily scores. That keeps it correct at any level (one line for a day, or the network
for a month).

## Pages

**1. Network overview**
- KPI cards: Reliability Score, Incidents, Mean Time To Recover, Live Alerts (each with
  `Reliability Change vs Previous 7 Days` as the trend)
- Line chart: Network Health Index by hour, last 7 days
- Bar chart: Reliability Score by line, sorted worst first, coloured by Risk Level
- Table: live alerts (line, status, reason, alert type)

**2. Line drill-through** (drill through on `dim_line[line_name]`)
- Daily Reliability Score trend with the 98 and 95 bands as constant lines
- Incident table: start, duration, peak severity, first reason
- Matrix: Incidents by hour of day × weekday (where do failures cluster?)

**3. Data quality**
- Data Coverage % by day
- Quarantined records by rule (`silver.quarantine`, Import)
- Pipeline runs (`ops.run_log`)

## Refresh

Publish to the Power BI service and schedule refresh hourly, 20 minutes past the hour (the
pipeline job runs at :07 and takes a few minutes). The DirectQuery alert table needs no refresh.
