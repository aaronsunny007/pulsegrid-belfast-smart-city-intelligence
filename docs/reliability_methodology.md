# How reliability is measured

## Input

Every 5 minutes the collector records the status TfL publishes for each line. TfL uses a
severity code from 0 to 20 (10 = Good Service). A line can report several statuses at once,
for example "Part Closure" for planned works plus "Minor Delays".

## Step 1: impact weight per status

Each severity gets an impact weight between 0 (no effect on passengers) and 1 (no service).
The codes are TfL's; the weights are this project's modelling choice and live in
`src/pulsecity/reference.py`.

| Example status | Weight | Planned by default |
|---|---|---|
| Good Service, No Issues, Information | 0.00 | no |
| No Step Free Access | 0.10 | no |
| Minor Delays, Issues Reported | 0.25 | no |
| Reduced Service, Diverted | 0.40 | no |
| Part Closure, Part Closed | 0.50 | yes |
| Severe Delays, Part Suspended | 0.60 | no |
| Suspended, Closed, Not Running | 1.00 | no |
| Planned Closure | 1.00 | yes |
| Service Closed (night) | excluded | - |

TfL's own `disruption.category` overrides the default: `PlannedWork` is planned, `RealTime`
is not.

## Step 2: one row per line per poll

For each poll, a line's **unplanned impact** is the highest weight among its unplanned statuses,
and its **planned impact** is the highest among planned ones. Each poll stands for the time until
the next poll. If the next poll is more than 15 minutes away (collector outage), the poll only
counts for one normal interval and the rest is treated as **unobserved**, not as good or bad
service.

## Step 3: daily score per line

```
reliability_score = 100 × (1 − Σ(duration × unplanned_impact) / Σ duration)
```

over operating time only (night closures are excluded).

Example: in one hour, a line has 10 minutes of Severe Delays (0.6) and is otherwise good.
Impact = 10 × 0.6 = 6 impact-minutes, so the score is 100 × (1 − 6/60) = **90.0**.

**Planned works do not lower the score.** They are reported separately as `planned_impact_min`.
The score answers "how well did the line run against its plan?", while planned works answer
"how much was the network closed for maintenance?". Mixing them would punish lines for
necessary engineering work.

## Step 4: risk level

| Score | Risk level |
|---|---|
| 98 and above | Stable |
| 95 to 98 | Moderate Risk |
| below 95 | High Risk |
| under 120 observed minutes in the day | Insufficient data |

The bands are in the city config. They are a starting point and should be recalibrated once a
few weeks of real history exist, for example by setting them at percentiles of the observed
score distribution.

## Incidents

Consecutive disrupted polls of a line form one **incident**: severe delays that ease to minor
delays count as one incident, not two. An incident ends when the line returns to an undisrupted
status, or at a data gap (we cannot claim an incident continued through time we did not
observe). The query uses `lag` and a running sum over a window (a "gaps and islands" pattern).

From incidents come **incident counts**, **mean time to recover** (average duration of incidents
that have ended) and the **longest incident** per day.

## Data coverage

`coverage_pct` = observed minutes ÷ time between the first and last poll of the day. Low
coverage means the collector missed polls. Read scores with low coverage with care.

## Known limitations

- Status is a published summary, not a measurement of train movements. A line can show
  "Good Service" while individual trains are late. A planned extension computes Excess Wait
  Time from TfL arrival predictions, which measures headway regularity directly.
- The impact weights are judgement calls. They are documented, versioned in code and applied
  consistently, so changing them recomputes the whole history the same way.
