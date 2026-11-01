import pytest
import requests

from pulsecity.sources.tfl import TflClient, TflError


class FakeResponse:
    def __init__(self, status, text="[]", headers=None):
        self.status_code = status
        self.text = text
        self.headers = headers or {}


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client(outcomes, **kw):
    sleeps = []
    session = FakeSession(outcomes)
    c = TflClient(session=session, sleep=sleeps.append, **kw)
    return c, session, sleeps


def test_line_status_path_joins_modes():
    c, session, _ = client([FakeResponse(200)])
    c.line_status(("tube", "dlr"))
    assert session.calls[0][0] == "https://api.tfl.gov.uk/Line/Mode/tube,dlr/Status"


def test_retries_on_server_errors_with_exponential_backoff():
    c, session, sleeps = client([FakeResponse(503), FakeResponse(502), FakeResponse(200, "[1]")])
    resp = c.get("/x")
    assert resp.status == 200 and resp.text == "[1]"
    assert sleeps == [2.0, 4.0]


def test_honours_retry_after_on_429():
    c, _, sleeps = client([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200)])
    assert c.get("/x").status == 200
    assert sleeps == [7.0]


def test_client_errors_are_returned_not_retried():
    c, session, sleeps = client([FakeResponse(404, "nope")])
    assert c.get("/x").status == 404
    assert len(session.calls) == 1 and sleeps == []


def test_raises_after_repeated_connection_errors():
    c, _, _ = client([requests.ConnectionError("down")] * 3, max_attempts=3)
    with pytest.raises(TflError, match="failed after 3 attempts"):
        c.get("/x")


def test_app_key_is_sent_but_never_stored_in_the_url():
    c, session, _ = client([FakeResponse(200)], app_key="secret-key")
    resp = c.get("/x")
    assert session.calls[0][1] == {"app_key": "secret-key"}
    assert "secret" not in resp.url
