# Data dictionary

All timestamps are stored in UTC. Columns ending in `_local` are London time.

## Bronze

### `bronze.line_status_raw` (one row per poll, append-only)

| Column | Description |
|---|---|
| snapshot_id | Unique id of the poll, e.g. `line_status-20261016T074500Z-1a2b3c4d` |
| city, source, dataset | `london`, `tfl`, `line_status` |
| endpoint | URL called (API key removed) |
| polled_at | When the poll was made |
| http_status | HTTP status; 0 means TfL could not be reached |
| elapsed_ms | Response time |
| error | Error message when the call failed |
| payload | Response body, exactly as received |
| _rescued_data | Anything in the landing file that did not match the envelope schema |
| _source_file, _ingested_at | Lineage: which file, when it was loaded |

## Silver

### `silver.line_status` (one row per poll, line and reported status)
Key: `snapshot_id, line_id, status_seq`

| Column | Description |
|---|---|
| line_id, line_name, mode | e.g. `central`, `Central`, `tube` |
| status_seq | Position of the status in TfL's list (a line can have several) |
| severity_code, severity_description | TfL severity, 0-20 |
| reason | TfL's free-text explanation, when disrupted |
| disruption_category | `RealTime`, `PlannedWork`, ... |
| closure_text | TfL closure code, e.g. `minorDelays` |
| valid_from, valid_to | First validity period TfL gave for the status |

### `silver.quarantine`

| Column | Description |
|---|---|
| stage, dataset | Where the record was rejected |
| snapshot_id | Poll it came from |
| failed_rules | Names of the rules it failed (see `src/pulsecity/quality.py`) |
| record_json | The full rejected record |
| quarantined_at | When |

## Gold

### `gold.fact_line_status` (one row per line per poll)

| Column | Description |
|---|---|
| polled_at, polled_at_local | Poll time, UTC and London |
| service_date | Operating day (starts 04:00 London time) |
| local_hour | Hour bucket, London time |
| worst_severity_code / _description / worst_reason / worst_is_planned | The most severe status this poll |
| unplanned_impact, planned_impact | 0-1 impact weights (see methodology) |
| is_disrupted | Unplanned impact > 0 while the line is operating |
| is_service_closed | Line shut for the night |
| status_count | How many statuses TfL reported |
| duration_min | Time this poll represents |
| data_gap_after | The next poll came too late; the gap is unobserved |

### `gold.fact_disruption_incident` (one row per incident)

| Column | Description |
|---|---|
| incident_id | Hash of line and start time |
| started_at, ended_at | Start and end (NULL while ongoing) |
| duration_min, polls | Length and number of polls |
| peak_severity_code / _description, peak_impact | Worst point of the incident |
| first_reason | TfL's explanation when it started |
| is_ongoing | Still disrupted at the latest poll |

### `gold.agg_line_reliability_daily` (one row per line per service day)

| Column | Description |
|---|---|
| polls, observed_min | How much of the day was observed |
| coverage_pct | Observed share of the day's polling window |
| good_service_min / good_service_pct | Time with no impact at all |
| disrupted_min | Time with any unplanned impact |
| unplanned_impact_min | Σ duration × unplanned impact |
| planned_impact_min | Σ duration × planned impact (does not affect the score) |
| incidents, mean_time_to_recover_min, longest_incident_min | Incident statistics |
| reliability_score | 100 × (1 − unplanned_impact_min / observed_min) |
| risk_level | Stable / Moderate Risk / High Risk / Insufficient data |

### `gold.agg_network_health_hourly` (one row per hour)

lines_observed, lines_disrupted, line_minutes_observed, network_health_index (time-weighted
score across all lines), planned_works_pct.

### `gold.line_status_current`
Latest poll per line. Powers the live view.

### Dimensions
- `gold.dim_line`: SCD Type 2. `line_sk`, `line_id`, `line_name`, `mode`, `valid_from`,
  `valid_to`, `is_current`. A renamed line keeps its old name in history.
- `gold.dim_status_severity`: TfL severities with this project's impact weights.
- `gold.dim_date`: calendar attributes for each service date.

### Views
- `gold.v_live_alerts`: lines currently not in good service.
- `gold.v_line_reliability_7d`: time-weighted 7-day score, incidents and MTTR per line.

## Ops

### `ops.run_log`
One row per pipeline step: run_id (Databricks job run), step, metrics_json, logged_at.
