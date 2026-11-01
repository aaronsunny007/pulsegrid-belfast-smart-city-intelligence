# Archive: PulseGrid Belfast (v1)

This folder keeps the first prototype for reference. It is **not** part of the current platform.

What v1 contained:

- A single call to the TfL `/Line/Mode/tube` endpoint (11 rows of line metadata, no operational data).
- Reliability scores (95 / 88 / 80 / 70) that were set by hand, not derived from data.
- A one-page Power BI report (`belfast.pbix`) built on those hand-set scores.

Why it was replaced: the scores could not be traced back to source data, the pipeline had no
history, tests or orchestration, and the name referred to Belfast while every row was a London line.
The rebuild in the repository root (PulseCity SmartGrid) replaces it with a scheduled collector,
a Bronze/Silver/Gold lakehouse and a reliability score computed from real service-status history.
