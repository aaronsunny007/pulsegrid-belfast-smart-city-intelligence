import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pulsecity.landing import collect_line_status, landing_path
from pulsecity.sources.tfl import ApiResponse, TflError

NOW = datetime(2026, 10, 16, 7, 45, tzinfo=timezone.utc)


class StubClient:
    def __init__(self, response=None, error=None):
        self.response, self.error = response, error

    def line_status(self, modes):
        if self.error:
            raise self.error
        return self.response


def test_landing_path_is_partitioned_by_utc_date():
    path = landing_path("/root", "london", "line_status", NOW, "snap1")
    assert path == "/root/london/line_status/ingest_date=2026-10-16/snap1.json"


def test_collect_writes_envelope_with_raw_payload(tmp_path):
    client = StubClient(ApiResponse("https://api.tfl.gov.uk/Line/Mode/tube/Status", 200, "[]", 120))
    landed = collect_line_status(
        client, city="london", source="tfl", modes=("tube",), landing_root=str(tmp_path), now=NOW
    )
    envelope = json.loads(Path(landed.path).read_text())
    assert envelope["http_status"] == 200
    assert envelope["payload"] == "[]"
    assert envelope["polled_at"] == "2026-10-16T07:45:00Z"
    # no temporary files left behind for Auto Loader to trip over
    assert [p.name for p in Path(landed.path).parent.iterdir()] == [Path(landed.path).name]


def test_collect_lands_an_error_envelope_and_reraises(tmp_path):
    with pytest.raises(TflError):
        collect_line_status(
            StubClient(error=TflError("boom")),
            city="london",
            source="tfl",
            modes=("tube",),
            landing_root=str(tmp_path),
            now=NOW,
        )
    (written,) = list(tmp_path.rglob("*.json"))
    envelope = json.loads(written.read_text())
    assert envelope["http_status"] == 0 and envelope["error"] == "boom"
    assert envelope["payload"] is None
