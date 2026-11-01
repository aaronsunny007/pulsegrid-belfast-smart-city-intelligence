"""Landing zone writer: one JSON file per API poll.

Each file is an "envelope": metadata about the poll plus the response body as raw text.
Files are written under a temporary dot-name and then renamed, so Auto Loader never
picks up a half-written file (Spark skips files whose names start with "." or "_").
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pulsecity.sources.tfl import ApiResponse, TflClient, TflError

LINE_STATUS = "line_status"


@dataclass(frozen=True)
class LandedFile:
    path: str
    snapshot_id: str
    http_status: int


def make_snapshot_id(dataset: str, polled_at: datetime) -> str:
    return f"{dataset}-{polled_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def build_envelope(
    *,
    snapshot_id: str,
    city: str,
    source: str,
    dataset: str,
    polled_at: datetime,
    response: ApiResponse | None,
    error: str | None = None,
) -> dict:
    return {
        "snapshot_id": snapshot_id,
        "city": city,
        "source": source,
        "dataset": dataset,
        "endpoint": response.url if response else None,
        "polled_at": polled_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "http_status": response.status if response else 0,
        "elapsed_ms": response.elapsed_ms if response else None,
        "error": error,
        "payload": response.text if response else None,
    }


def landing_path(root: str, city: str, dataset: str, polled_at: datetime, snapshot_id: str) -> str:
    day = polled_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return f"{root}/{city}/{dataset}/ingest_date={day}/{snapshot_id}.json"


def write_atomic(path: str, envelope: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    tmp.write_text(json.dumps(envelope, separators=(",", ":")))
    os.replace(tmp, target)


def collect_line_status(
    client: TflClient,
    *,
    city: str,
    source: str,
    modes: tuple[str, ...],
    landing_root: str,
    now: datetime | None = None,
) -> LandedFile:
    """Poll line status once and land the result.

    If TfL cannot be reached, an envelope with ``http_status = 0`` is still landed
    (so the gap is visible in the data) and the error is re-raised so the job run fails
    and alerts fire.
    """
    polled_at = now or datetime.now(timezone.utc)
    snapshot_id = make_snapshot_id(LINE_STATUS, polled_at)
    path = landing_path(landing_root, city, LINE_STATUS, polled_at, snapshot_id)

    try:
        response = client.line_status(modes)
    except TflError as exc:
        envelope = build_envelope(
            snapshot_id=snapshot_id,
            city=city,
            source=source,
            dataset=LINE_STATUS,
            polled_at=polled_at,
            response=None,
            error=str(exc),
        )
        write_atomic(path, envelope)
        raise

    envelope = build_envelope(
        snapshot_id=snapshot_id,
        city=city,
        source=source,
        dataset=LINE_STATUS,
        polled_at=polled_at,
        response=response,
    )
    write_atomic(path, envelope)
    return LandedFile(path=path, snapshot_id=snapshot_id, http_status=response.status)
