from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


class ApiSportsRateLimitError(RuntimeError):
    """Raised when the local or remote quota gate blocks a request."""


@dataclass(frozen=True)
class ApiSportsRateLimitState:
    requests_limit: int | None = None
    requests_remaining: int | None = None
    per_minute_limit: int | None = None
    per_minute_remaining: int | None = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, object]) -> "ApiSportsRateLimitState":
        lower = {str(key).lower(): str(value) for key, value in headers.items()}
        return cls(
            requests_limit=_safe_int(lower.get("x-ratelimit-requests-limit")),
            requests_remaining=_safe_int(lower.get("x-ratelimit-requests-remaining")),
            per_minute_limit=_safe_int(lower.get("x-ratelimit-limit")),
            per_minute_remaining=_safe_int(lower.get("x-ratelimit-remaining")),
        )

    def assert_safe(self, *, min_remaining_requests: int) -> None:
        if self.requests_remaining is not None and self.requests_remaining < min_remaining_requests:
            raise ApiSportsRateLimitError(
                "API-Sports quota guard blocked request: "
                f"remaining={self.requests_remaining}, minimum={min_remaining_requests}"
            )

        if self.per_minute_remaining is not None and self.per_minute_remaining <= 0:
            raise ApiSportsRateLimitError("API-Sports per-minute quota exhausted.")


def _safe_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None
