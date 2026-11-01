-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PulseCity SmartGrid: reliability analysis
-- MAGIC Ad-hoc Spark SQL over the Gold layer. Set the catalog in the widget (`pulsecity` for prod,
-- MAGIC `pulsecity_dev` for dev). Every query here reads Gold tables built by the `pulsecity-pipeline` job.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'pulsecity';
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- MAGIC %md ## Which lines were least reliable over the last 7 days?

-- COMMAND ----------

SELECT line_name, reliability_score_7d, incidents_7d, avg_time_to_recover_min,
       planned_works_hours_7d, avg_coverage_pct
FROM gold.v_line_reliability_7d
ORDER BY reliability_score_7d;

-- COMMAND ----------

-- MAGIC %md ## When in the day do incidents start? (local time)

-- COMMAND ----------

SELECT hour(from_utc_timestamp(started_at, 'Europe/London')) AS local_hour,
       count(*) AS incidents,
       round(avg(duration_min), 1) AS avg_duration_min
FROM gold.fact_disruption_incident
GROUP BY local_hour
ORDER BY local_hour;

-- COMMAND ----------

-- MAGIC %md ## Weekday vs weekend reliability

-- COMMAND ----------

SELECT d.is_weekend, r.line_name,
       round(100 * (1 - sum(r.unplanned_impact_min) / sum(r.observed_min)), 2) AS reliability_score,
       sum(r.incidents) AS incidents
FROM gold.agg_line_reliability_daily r
JOIN gold.dim_date d ON d.date = r.service_date
GROUP BY d.is_weekend, r.line_name
ORDER BY r.line_name, d.is_weekend;

-- COMMAND ----------

-- MAGIC %md ## Longest incidents and what TfL said at the start

-- COMMAND ----------

SELECT line_name, started_at, duration_min, peak_severity_description, first_reason
FROM gold.fact_disruption_incident
WHERE NOT is_ongoing
ORDER BY duration_min DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Network health through the day (rolling 3-hour average)

-- COMMAND ----------

SELECT local_hour, network_health_index, lines_disrupted,
       round(avg(network_health_index) OVER (ORDER BY local_hour
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3h
FROM gold.agg_network_health_hourly
ORDER BY local_hour DESC
LIMIT 72;

-- COMMAND ----------

-- MAGIC %md ## Data quality: collector coverage and quarantined records

-- COMMAND ----------

SELECT service_date, round(avg(coverage_pct), 1) AS avg_coverage_pct, sum(polls) AS polls
FROM gold.agg_line_reliability_daily
GROUP BY service_date
ORDER BY service_date DESC;

-- COMMAND ----------

SELECT date(quarantined_at) AS day, stage, failed_rules, count(*) AS records
FROM silver.quarantine
GROUP BY ALL
ORDER BY day DESC;

-- COMMAND ----------

-- MAGIC %md ## Pipeline run log and Delta table history

-- COMMAND ----------

SELECT * FROM ops.run_log ORDER BY logged_at DESC LIMIT 20;

-- COMMAND ----------

DESCRIBE HISTORY gold.fact_line_status;

-- COMMAND ----------

-- MAGIC %md ## Line name history (SCD Type 2)

-- COMMAND ----------

SELECT line_id, line_name, mode, valid_from, valid_to, is_current
FROM gold.dim_line
ORDER BY line_id, valid_from;
