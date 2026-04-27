from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_APISPORTS_BASE_URL = "https://v3.football.api-sports.io"


class ApiSportsConfigError(RuntimeError):
    """Raised when API-Sports configuration is invalid."""


@dataclass(frozen=True)
class ApiSportsConfig:
    api_key: str
    base_url: str = DEFAULT_APISPORTS_BASE_URL
    timeout_seconds: float = 20.0
    daily_budget: int = 75_000
    min_remaining_requests: int = 1_000
    cache_dir: Path = Path("data/cache/api_sports")

    @classmethod
    def from_env(cls, *, require_key: bool = True) -> "ApiSportsConfig":
        api_key = os.getenv("APISPORTS_KEY", "").strip()
        if require_key and not api_key:
            raise ApiSportsConfigError(
                "APISPORTS_KEY is missing. Add it to .env or environment variables. "
                "Never commit the key."
            )

        base_url = os.getenv("APISPORTS_BASE_URL", DEFAULT_APISPORTS_BASE_URL).strip()
        timeout_seconds = _env_float("APISPORTS_TIMEOUT_SECONDS", 20.0)
        daily_budget = _env_int("APISPORTS_DAILY_BUDGET", 75_000)
        min_remaining_requests = _env_int("APISPORTS_MIN_REMAINING_REQUESTS", 1_000)
        cache_dir = Path(os.getenv("APISPORTS_CACHE_DIR", "data/cache/api_sports").strip())

        if not base_url:
            raise ApiSportsConfigError("APISPORTS_BASE_URL cannot be empty.")
        if timeout_seconds <= 0:
            raise ApiSportsConfigError("APISPORTS_TIMEOUT_SECONDS must be positive.")
        if daily_budget <= 0:
            raise ApiSportsConfigError("APISPORTS_DAILY_BUDGET must be positive.")
        if min_remaining_requests < 0:
            raise ApiSportsConfigError("APISPORTS_MIN_REMAINING_REQUESTS cannot be negative.")

        return cls(
            api_key=api_key,
            base_url=base_url.rstrip("/"),
            timeout_seconds=timeout_seconds,
            daily_budget=daily_budget,
            min_remaining_requests=min_remaining_requests,
            cache_dir=cache_dir,
        )

    @property
    def headers(self) -> dict[str, str]:
        return {
            "x-apisports-key": self.api_key,
            "Accept": "application/json",
        }

    def redacted(self) -> dict[str, object]:
        return {
            "api_key": "***REDACTED***" if self.api_key else "",
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "daily_budget": self.daily_budget,
            "min_remaining_requests": self.min_remaining_requests,
            "cache_dir": str(self.cache_dir),
        }


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsConfigError(f"{name} must be an integer.") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsConfigError(f"{name} must be a float.") from exc
