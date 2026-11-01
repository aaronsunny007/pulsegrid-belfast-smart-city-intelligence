"""SYNTHETIC TfL-shaped data for tests and the offline demo.

Nothing here is real TfL data. It mimics the shape of /Line/Mode/{modes}/Status responses
so the pipeline can be exercised end to end without network access or an Azure workspace.
Real data only ever comes from pulsecity.landing.collect_line_status.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from pulsecity.landing import LINE_STATUS, build_envelope, landing_path, write_atomic
from pulsecity.reference import SEVERITIES
from pulsecity.sources.tfl import ApiResponse

LONDON = ZoneInfo("Europe/London")
_DESCRIPTIONS = {code: desc for code, desc, *_ in SEVERITIES}

LINES: tuple[tuple[str, str, str], ...] = (
    ("bakerloo", "Bakerloo", "tube"),
    ("central", "Central", "tube"),
    ("circle", "Circle", "tube"),
    ("district", "District", "tube"),
    ("hammersmith-city", "Hammersmith & City", "tube"),
    ("jubilee", "Jubilee", "tube"),
    ("metropolitan", "Metropolitan", "tube"),
    ("northern", "Northern", "tube"),
    ("piccadilly", "Piccadilly", "tube"),
    ("victoria", "Victoria", "tube"),
    ("waterloo-city", "Waterloo & City", "tube"),
    ("elizabeth", "Elizabeth line", "elizabeth-line"),
    ("dlr", "DLR", "dlr"),
)
NIGHT_TUBE = {"central", "jubilee", "northern", "piccadilly", "victoria"}


@dataclass(frozen=True)
class Event:
    line_id: str
    day: date  # local calendar date of the start
    start: str  # "HH:MM" local
    end: str  # "HH:MM" local (same day)
    severity: int
    category: str  # "RealTime" or "PlannedWork"
    reason: str

    def covers(self, local: datetime) -> bool:
        s = datetime.combine(self.day, time.fromisoformat(self.start), LONDON)
        e = datetime.combine(self.day, time.fromisoformat(self.end), LONDON)
        return s <= local < e


def status(severity: int, line_id: str, reason: str | None = None, category: str | None = None):
    s = {
        "$type": "Tfl.Api.Presentation.Entities.LineStatus, Tfl.Api.Presentation.Entities",
        "id": 0,
        "statusSeverity": severity,
        "statusSeverityDescription": _DESCRIPTIONS[severity],
        "created": "0001-01-01T00:00:00",
        "validityPeriods": [],
    }
    if reason:
        s.update(
            lineId=line_id,
            reason=reason,
            disruption={
                "$type": "Tfl.Api.Presentation.Entities.Disruption, Tfl.Api.Presentation.Entities",
                "category": category,
                "categoryDescription": category,
                "description": reason,
                "affectedRoutes": [],
                "affectedStops": [],
                "closureText": _DESCRIPTIONS[severity].replace(" ", "").lower(),
            },
        )
    return s


def line(line_id: str, name: str, mode: str, statuses: list[dict]) -> dict:
    return {
        "$type": "Tfl.Api.Presentation.Entities.Line, Tfl.Api.Presentation.Entities",
        "id": line_id,
        "name": name,
        "modeName": mode,
        "disruptions": [],
        "lineStatuses": statuses,
        "routeSections": [],
        "serviceTypes": [],
        "crowding": {},
    }


def is_night_closed(line_id: str, local: datetime) -> bool:
    t = local.time()
    if not (time(0, 30) <= t < time(5, 30)):
        return False
    evening_before = (local - timedelta(days=1)).date()
    night_tube_night = evening_before.weekday() in (4, 5)  # Friday or Saturday night
    return not (night_tube_night and line_id in NIGHT_TUBE)


def payload_at(local: datetime, events: list[Event]) -> list[dict]:
    lines = []
    for line_id, name, mode in LINES:
        active = [e for e in events if e.line_id == line_id and e.covers(local)]
        if is_night_closed(line_id, local):
            statuses = [status(20, line_id)]
        elif active:
            statuses = [status(e.severity, line_id, e.reason, e.category) for e in active]
        else:
            statuses = [status(10, line_id)]
        lines.append(line(line_id, name, mode, statuses))
    return lines


# A two-day scenario: a Friday with real-time incidents, a collector outage and a failed poll,
# then a Saturday with planned engineering works (which must NOT lower reliability).
FRIDAY = date(2026, 10, 16)
SATURDAY = date(2026, 10, 17)
DEFAULT_EVENTS: tuple[Event, ...] = (
    Event(
        "northern",
        FRIDAY,
        "07:40",
        "08:35",
        9,
        "RealTime",
        "NORTHERN LINE: Minor delays due to an earlier signal failure at Camden Town.",
    ),
    Event(
        "jubilee",
        FRIDAY,
        "09:00",
        "09:20",
        9,
        "RealTime",
        "JUBILEE LINE: Minor delays due to a train cancellation.",
    ),
    Event(
        "district",
        FRIDAY,
        "12:00",
        "12:25",
        3,
        "RealTime",
        "DISTRICT LINE: No service between Earl's Court and Wimbledon due to a customer incident.",
    ),
    Event(
        "jubilee",
        FRIDAY,
        "16:30",
        "16:45",
        9,
        "RealTime",
        "JUBILEE LINE: Minor delays due to a late finish of engineering works.",
    ),
    Event(
        "central",
        FRIDAY,
        "17:10",
        "17:55",
        6,
        "RealTime",
        "CENTRAL LINE: Severe delays due to a faulty train at Liverpool Street.",
    ),
    Event(
        "central",
        FRIDAY,
        "17:55",
        "18:20",
        9,
        "RealTime",
        "CENTRAL LINE: Minor delays while we recover from a faulty train earlier.",
    ),
    Event(
        "elizabeth",
        FRIDAY,
        "19:00",
        "19:40",
        6,
        "RealTime",
        "ELIZABETH LINE: Severe delays due to a points failure at Stratford.",
    ),
    Event(
        "jubilee",
        SATURDAY,
        "05:30",
        "23:59",
        5,
        "PlannedWork",
        "JUBILEE LINE: No service between Finchley Road and Stratford due to planned "
        "engineering works.",
    ),
    Event(
        "piccadilly",
        SATURDAY,
        "11:00",
        "12:10",
        6,
        "RealTime",
        "PICCADILLY LINE: Severe delays due to a signal failure at Acton Town.",
    ),
    Event(
        "victoria",
        SATURDAY,
        "21:00",
        "21:30",
        9,
        "RealTime",
        "VICTORIA LINE: Minor delays due to a customer incident at Oxford Circus.",
    ),
)
# Collector problems on Friday (local times): no polls at all, and one HTTP 503 poll.
OUTAGE = (time(14, 0), time(14, 30))
FAILED_POLL = time(15, 0)


def generate_landing(
    landing_root: str,
    city: str = "london",
    days: tuple[date, ...] = (FRIDAY, SATURDAY),
    interval_min: int = 5,
    events: tuple[Event, ...] = DEFAULT_EVENTS,
) -> int:
    """Write synthetic envelopes for whole service days (04:00 to 04:00 local)."""
    count = 0
    for day in days:
        local = datetime.combine(day, time(4, 0), LONDON)
        end = local + timedelta(days=1)
        while local < end:
            # Recompute from UTC so the clock-change night stays correct.
            polled_at = local.astimezone(ZoneInfo("UTC"))
            in_outage = day == FRIDAY and OUTAGE[0] <= local.time() < OUTAGE[1]
            if not in_outage:
                failed = day == FRIDAY and local.time() == FAILED_POLL
                body = (
                    '{"message":"Service Unavailable"}'
                    if failed
                    else json.dumps(payload_at(local, list(events)))
                )
                response = ApiResponse(
                    url="https://api.tfl.gov.uk/Line/Mode/tube,elizabeth-line,dlr/Status",
                    status=503 if failed else 200,
                    text=body,
                    elapsed_ms=180,
                )
                snapshot_id = f"{LINE_STATUS}-{polled_at:%Y%m%dT%H%M%SZ}-synthetic"
                envelope = build_envelope(
                    snapshot_id=snapshot_id,
                    city=city,
                    source="tfl",
                    dataset=LINE_STATUS,
                    polled_at=polled_at,
                    response=response,
                )
                write_atomic(
                    landing_path(landing_root, city, LINE_STATUS, polled_at, snapshot_id),
                    envelope,
                )
                count += 1
            local = (polled_at + timedelta(minutes=interval_min)).astimezone(LONDON)
    return count
