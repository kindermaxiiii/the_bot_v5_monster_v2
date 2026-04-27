from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class ApiSportsCacheEntry:
    key: str
    path: Path
    created_at: float
    payload: Mapping[str, Any]


class ApiSportsJsonCache:
    def __init__(self, root: Path) -> None:
        self.root = root

    def get(self, endpoint: str, params: Mapping[str, Any], *, ttl_seconds: int) -> Mapping[str, Any] | None:
        path = self._path(endpoint, params)
        if not path.exists():
            return None

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

        if ttl_seconds < 0:
            return None

        created_at = _safe_float(data.get("_created_at"), default=0.0)
        if time.time() - created_at > ttl_seconds:
            return None

        payload = data.get("payload")
        return payload if isinstance(payload, dict) else None

    def set(self, endpoint: str, params: Mapping[str, Any], payload: Mapping[str, Any]) -> Path:
        path = self._path(endpoint, params)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "_created_at": time.time(),
            "endpoint": endpoint,
            "params": dict(params),
            "payload": dict(payload),
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _path(self, endpoint: str, params: Mapping[str, Any]) -> Path:
        safe_endpoint = endpoint.replace("/", "__")
        digest_input = json.dumps(
            {"endpoint": endpoint, "params": dict(params)},
            sort_keys=True,
            default=str,
        )
        digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:24]
        return self.root / safe_endpoint / f"{digest}.json"


def _safe_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
