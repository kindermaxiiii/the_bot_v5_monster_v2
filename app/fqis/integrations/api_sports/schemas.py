from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


class ApiSportsResponseError(RuntimeError):
    """Raised when API-Sports returns an error payload."""


@dataclass(frozen=True)
class ApiSportsPaging:
    current: int | None = None
    total: int | None = None


@dataclass(frozen=True)
class ApiSportsResponse:
    endpoint: str
    parameters: Any
    errors: Any
    results: int | None
    paging: ApiSportsPaging
    response: Any
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ApiSportsResponse":
        errors = payload.get("errors", [])
        if _has_errors(errors):
            raise ApiSportsResponseError(f"API-Sports returned errors: {errors}")

        paging_raw = payload.get("paging") or {}
        if not isinstance(paging_raw, Mapping):
            paging_raw = {}

        return cls(
            endpoint=str(payload.get("get", "")),
            parameters=payload.get("parameters") or {},
            errors=errors,
            results=_safe_int(payload.get("results")),
            paging=ApiSportsPaging(
                current=_safe_int(paging_raw.get("current")),
                total=_safe_int(paging_raw.get("total")),
            ),
            response=payload.get("response"),
            raw=payload,
        )


def _has_errors(errors: Any) -> bool:
    if errors is None:
        return False
    if isinstance(errors, list):
        return len(errors) > 0
    if isinstance(errors, dict):
        return len(errors) > 0
    if isinstance(errors, str):
        return bool(errors.strip())
    return bool(errors)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
