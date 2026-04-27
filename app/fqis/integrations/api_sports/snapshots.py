from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol

from app.fqis.integrations.api_sports.client import ApiSportsClient


class ApiSportsSnapshotSecurityError(RuntimeError):
    """Raised when a raw payload appears to contain credentials or secret-bearing fields."""


class ApiSportsSnapshotKind(str, Enum):
    FIXTURES_BY_DATE = "fixtures_by_date"
    ODDS_BY_DATE = "odds_by_date"
    LIVE_FIXTURES = "live_fixtures"
    LIVE_ODDS = "live_odds"


@dataclass(frozen=True)
class ApiSportsSnapshotRecord:
    snapshot_id: str
    run_id: str
    kind: str
    endpoint: str
    params: Mapping[str, Any]
    captured_at_utc: str
    path: Path
    response_results: int | None = None
    response_items: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "kind": self.kind,
            "endpoint": self.endpoint,
            "params": dict(self.params),
            "captured_at_utc": self.captured_at_utc,
            "path": str(self.path),
            "response_results": self.response_results,
            "response_items": self.response_items,
        }


@dataclass(frozen=True)
class ApiSportsSnapshotManifest:
    status: str
    run_id: str
    provider: str
    mode: str
    date: str
    timezone: str
    captured_at_utc: str
    snapshots: tuple[ApiSportsSnapshotRecord, ...]
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def snapshot_count(self) -> int:
        return len(self.snapshots)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "provider": self.provider,
            "mode": self.mode,
            "date": self.date,
            "timezone": self.timezone,
            "captured_at_utc": self.captured_at_utc,
            "snapshot_count": self.snapshot_count,
            "warnings": list(self.warnings),
            "snapshots": [snapshot.to_dict() for snapshot in self.snapshots],
        }


class _ApiSportsResponseLike(Protocol):
    endpoint: str
    results: int | None
    response: Any
    raw: Mapping[str, Any]
    paging: Any


class ApiSportsSnapshotWriter:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write(
        self,
        *,
        kind: ApiSportsSnapshotKind | str,
        endpoint: str,
        params: Mapping[str, Any],
        raw_payload: Mapping[str, Any],
        run_id: str,
        captured_at_utc: str | None = None,
        provider: str = "api_sports_api_football",
        metadata: Mapping[str, Any] | None = None,
    ) -> ApiSportsSnapshotRecord:
        _assert_no_secret_keys(raw_payload)
        _assert_no_secret_keys(params)
        if metadata is not None:
            _assert_no_secret_keys(metadata)

        normalized_kind = kind.value if isinstance(kind, ApiSportsSnapshotKind) else str(kind)
        captured_at = captured_at_utc or _utc_now_iso()
        clean_params = dict(params)
        payload_dict = dict(raw_payload)
        snapshot_id = _snapshot_id(
            provider=provider,
            run_id=run_id,
            kind=normalized_kind,
            endpoint=endpoint,
            params=clean_params,
            raw_payload=payload_dict,
            captured_at_utc=captured_at,
        )

        envelope = {
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "provider": provider,
            "mode": "shadow_only_raw_snapshot",
            "kind": normalized_kind,
            "endpoint": endpoint,
            "params": clean_params,
            "captured_at_utc": captured_at,
            "metadata": dict(metadata or {}),
            "secret_policy": {
                "api_key": "***REDACTED***",
                "secrets_included": False,
            },
            "raw_payload": payload_dict,
        }

        path = self._path(captured_at, run_id, normalized_kind, snapshot_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

        return ApiSportsSnapshotRecord(
            snapshot_id=snapshot_id,
            run_id=run_id,
            kind=normalized_kind,
            endpoint=endpoint,
            params=clean_params,
            captured_at_utc=captured_at,
            path=path,
            response_results=_safe_int(payload_dict.get("results")),
            response_items=_response_items(payload_dict.get("response")),
        )

    def _path(self, captured_at_utc: str, run_id: str, kind: str, snapshot_id: str) -> Path:
        day = captured_at_utc[:10] if captured_at_utc else "unknown-date"
        safe_run_id = _safe_path_part(run_id)
        safe_kind = _safe_path_part(kind)
        return self.root / day / safe_run_id / f"{safe_kind}-{snapshot_id[:16]}.json"


class ApiSportsSnapshotCollector:
    def __init__(self, client: ApiSportsClient, writer: ApiSportsSnapshotWriter) -> None:
        self.client = client
        self.writer = writer

    def collect_date(
        self,
        *,
        date: str,
        timezone: str = "Europe/Paris",
        include_odds: bool = True,
        include_live: bool = False,
        max_odds_pages: int = 5,
        run_id: str | None = None,
    ) -> ApiSportsSnapshotManifest:
        if max_odds_pages < 1:
            raise ValueError("max_odds_pages must be >= 1")

        capture_run_id = run_id or _new_run_id()
        captured_at = _utc_now_iso()
        snapshots: list[ApiSportsSnapshotRecord] = []
        warnings: list[str] = []

        fixtures = self.client.fixtures_by_date(date, timezone)
        snapshots.append(
            self._write_response(
                kind=ApiSportsSnapshotKind.FIXTURES_BY_DATE,
                response=fixtures,
                params={"date": date, "timezone": timezone},
                run_id=capture_run_id,
                captured_at_utc=captured_at,
            )
        )

        if include_odds:
            first_odds = self.client.odds_by_date(date, timezone, page=1)
            snapshots.append(
                self._write_response(
                    kind=ApiSportsSnapshotKind.ODDS_BY_DATE,
                    response=first_odds,
                    params={"date": date, "timezone": timezone, "page": 1},
                    run_id=capture_run_id,
                    captured_at_utc=captured_at,
                )
            )

            total_pages = _safe_int(getattr(first_odds.paging, "total", None)) or 1
            if total_pages > max_odds_pages:
                warnings.append(
                    f"odds pagination capped: total_pages={total_pages}, max_odds_pages={max_odds_pages}"
                )
            pages_to_fetch = min(total_pages, max_odds_pages)
            for page in range(2, pages_to_fetch + 1):
                odds_page = self.client.odds_by_date(date, timezone, page=page)
                snapshots.append(
                    self._write_response(
                        kind=ApiSportsSnapshotKind.ODDS_BY_DATE,
                        response=odds_page,
                        params={"date": date, "timezone": timezone, "page": page},
                        run_id=capture_run_id,
                        captured_at_utc=captured_at,
                    )
                )

        if include_live:
            live_fixtures = self.client.live_fixtures()
            snapshots.append(
                self._write_response(
                    kind=ApiSportsSnapshotKind.LIVE_FIXTURES,
                    response=live_fixtures,
                    params={"live": "all"},
                    run_id=capture_run_id,
                    captured_at_utc=captured_at,
                )
            )

            live_odds = self.client.live_odds()
            snapshots.append(
                self._write_response(
                    kind=ApiSportsSnapshotKind.LIVE_ODDS,
                    response=live_odds,
                    params={},
                    run_id=capture_run_id,
                    captured_at_utc=captured_at,
                )
            )

        return ApiSportsSnapshotManifest(
            status="COMPLETED",
            run_id=capture_run_id,
            provider="api_sports_api_football",
            mode="shadow_only_fixtures_odds_snapshot",
            date=date,
            timezone=timezone,
            captured_at_utc=captured_at,
            snapshots=tuple(snapshots),
            warnings=tuple(warnings),
        )

    def _write_response(
        self,
        *,
        kind: ApiSportsSnapshotKind,
        response: _ApiSportsResponseLike,
        params: Mapping[str, Any],
        run_id: str,
        captured_at_utc: str,
    ) -> ApiSportsSnapshotRecord:
        return self.writer.write(
            kind=kind,
            endpoint=response.endpoint,
            params=params,
            raw_payload=response.raw,
            run_id=run_id,
            captured_at_utc=captured_at_utc,
            metadata={
                "response_results": response.results,
                "response_items": _response_items(response.response),
            },
        )


def _snapshot_id(
    *,
    provider: str,
    run_id: str,
    kind: str,
    endpoint: str,
    params: Mapping[str, Any],
    raw_payload: Mapping[str, Any],
    captured_at_utc: str,
) -> str:
    digest_input = json.dumps(
        {
            "provider": provider,
            "run_id": run_id,
            "kind": kind,
            "endpoint": endpoint,
            "params": dict(params),
            "captured_at_utc": captured_at_utc,
            "raw_payload": dict(raw_payload),
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def _assert_no_secret_keys(value: Any, *, path: str = "payload") -> None:
    forbidden = {
        "api_key",
        "apikey",
        "apisports_key",
        "apisports_api_key",
        "x-apisports-key",
        "authorization",
        "access_token",
        "token",
        "secret",
    }

    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized_key = str(key).strip().lower().replace("-", "_")
            if normalized_key in {item.replace("-", "_") for item in forbidden}:
                raise ApiSportsSnapshotSecurityError(f"Forbidden secret-like key in snapshot {path}.{key}")
            _assert_no_secret_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_no_secret_keys(nested, path=f"{path}[{index}]")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"api-sports-snapshot-{timestamp}-{uuid.uuid4().hex[:8]}"


def _safe_path_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip())
    return cleaned or "unknown"


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _response_items(response: Any) -> int | None:
    if isinstance(response, list):
        return len(response)
    if isinstance(response, Mapping):
        return len(response)
    return None
