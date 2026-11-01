"""Small client for the TfL Unified API (https://api.tfl.gov.uk).

It returns the raw response text untouched, so Bronze stores exactly what TfL sent.
Parsing happens later, in Silver, against an explicit schema.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import requests

BASE_URL = "https://api.tfl.gov.uk"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class ApiResponse:
    url: str  # with the app_key removed, so it is safe to store
    status: int
    text: str
    elapsed_ms: int


class TflError(RuntimeError):
    """Raised when TfL could not be reached after all retries."""


class TflClient:
    def __init__(
        self,
        app_key: str | None = None,
        *,
        session: requests.Session | None = None,
        base_url: str = BASE_URL,
        timeout_s: float = 20.0,
        max_attempts: int = 4,
        backoff_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.app_key = app_key or None
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self._sleep = sleep

    # Endpoints -----------------------------------------------------------------------------

    def line_status_path(self, modes: Iterable[str]) -> str:
        return f"/Line/Mode/{','.join(modes)}/Status"

    def line_status(self, modes: Iterable[str]) -> ApiResponse:
        """Current status of every line in the given modes (one call for all of them)."""
        return self.get(self.line_status_path(modes))

    # Transport -----------------------------------------------------------------------------

    def get(self, path: str) -> ApiResponse:
        url = f"{self.base_url}{path}"
        params = {"app_key": self.app_key} if self.app_key else None
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_s)
            except requests.RequestException as exc:  # DNS, TLS, timeouts, resets
                last_error = exc
            else:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                if resp.status_code not in RETRYABLE_STATUS or attempt == self.max_attempts:
                    return ApiResponse(url, resp.status_code, resp.text, elapsed_ms)
                last_error = TflError(f"HTTP {resp.status_code}")
                retry_after = _retry_after_seconds(resp)
                if retry_after is not None:
                    self._sleep(retry_after)
                    continue

            if attempt < self.max_attempts:
                self._sleep(self.backoff_s * 2 ** (attempt - 1))

        raise TflError(f"GET {url} failed after {self.max_attempts} attempts: {last_error}")


def _retry_after_seconds(resp: requests.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return min(float(value), 60.0)
    except ValueError:
        return None
