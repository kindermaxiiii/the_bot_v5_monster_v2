
from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.run_ledger import (
    ApiSportsRunLedgerError,
    default_run_ledger_path,
    read_run_ledger,
)


@dataclass(frozen=True)
class ApiSportsRunRegistryEntry:
    run_id: str
    status: str
    ready: bool
    ledger_key: str | None
    manifest_path: str | None
    manifest_sha256: str | None
    run_dir: str | None
    normalized_input: str | None
    payload_sha256: str | None
    started_at_utc: str | None
    completed_at_utc: str | None
    steps_total: int
    steps_completed: int
    steps_failed: int
    errors_total: int
    quality_status: str | None
    quality_ready: bool | None
    quality_issues_total: int | None
    raw: Mapping[str, Any]

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "ApiSportsRunRegistryEntry":
        run_id = _required_str(record.get("run_id"), "run_id")
        status = _upper(_required_str(record.get("status"), "status"))

        return cls(
            run_id=run_id,
            status=status,
            ready=_bool(record.get("ready")),
            ledger_key=_optional_str(record.get("ledger_key")),
            manifest_path=_optional_str(record.get("manifest_path")),
            manifest_sha256=_optional_str(record.get("manifest_sha256")),
            run_dir=_optional_str(record.get("run_dir")),
            normalized_input=_optional_str(record.get("normalized_input")),
            payload_sha256=_optional_str(record.get("payload_sha256")),
            started_at_utc=_optional_str(record.get("started_at_utc")),
            completed_at_utc=_optional_str(record.get("completed_at_utc")),
            steps_total=_int(record.get("steps_total")),
            steps_completed=_int(record.get("steps_completed")),
            steps_failed=_int(record.get("steps_failed")),
            errors_total=_int(record.get("errors_total")),
            quality_status=_upper_or_none(record.get("quality_status")),
            quality_ready=_bool_or_none(record.get("quality_ready")),
            quality_issues_total=_int_or_none(record.get("quality_issues_total")),
            raw=dict(record),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "ready": self.ready,
            "ledger_key": self.ledger_key,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "run_dir": self.run_dir,
            "normalized_input": self.normalized_input,
            "payload_sha256": self.payload_sha256,
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "steps_total": self.steps_total,
            "steps_completed": self.steps_completed,
            "steps_failed": self.steps_failed,
            "errors_total": self.errors_total,
            "quality_status": self.quality_status,
            "quality_ready": self.quality_ready,
            "quality_issues_total": self.quality_issues_total,
        }


@dataclass(frozen=True)
class ApiSportsRunRegistrySnapshot:
    ledger_path: str
    entries_total: int
    ready_total: int
    status_counts: Mapping[str, int]
    quality_status_counts: Mapping[str, int]
    latest_run_id: str | None
    latest_ready_run_id: str | None
    latest_completed_run_id: str | None
    latest_failed_run_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "ledger_path": self.ledger_path,
            "entries_total": self.entries_total,
            "ready_total": self.ready_total,
            "status_counts": dict(self.status_counts),
            "quality_status_counts": dict(self.quality_status_counts),
            "latest_run_id": self.latest_run_id,
            "latest_ready_run_id": self.latest_ready_run_id,
            "latest_completed_run_id": self.latest_completed_run_id,
            "latest_failed_run_id": self.latest_failed_run_id,
        }


@dataclass(frozen=True)
class ApiSportsRunRegistrySelection:
    status: str
    ledger_path: str
    criteria: Mapping[str, object]
    matches_total: int
    entry: ApiSportsRunRegistryEntry | None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "ledger_path": self.ledger_path,
            "criteria": dict(self.criteria),
            "matches_total": self.matches_total,
            "entry": self.entry.to_dict() if self.entry is not None else None,
        }


class ApiSportsRunRegistryError(RuntimeError):
    pass


class ApiSportsRunRegistry:
    def __init__(self, ledger_path: str | Path | None = None) -> None:
        self.ledger_path = Path(ledger_path) if ledger_path is not None else default_run_ledger_path()
        self._entries: tuple[ApiSportsRunRegistryEntry, ...] | None = None

    @property
    def entries(self) -> tuple[ApiSportsRunRegistryEntry, ...]:
        if self._entries is None:
            self._entries = tuple(
                ApiSportsRunRegistryEntry.from_record(record)
                for record in read_run_ledger(self.ledger_path)
            )
        return self._entries

    def snapshot(self) -> ApiSportsRunRegistrySnapshot:
        entries = self.entries
        latest = entries[-1] if entries else None
        latest_ready = self.latest(ready=True)
        latest_completed = self.latest(status="COMPLETED")
        latest_failed = self.latest(status="FAILED")

        return ApiSportsRunRegistrySnapshot(
            ledger_path=str(self.ledger_path),
            entries_total=len(entries),
            ready_total=sum(1 for entry in entries if entry.ready),
            status_counts=dict(Counter(entry.status for entry in entries)),
            quality_status_counts=dict(Counter(entry.quality_status for entry in entries if entry.quality_status)),
            latest_run_id=latest.run_id if latest else None,
            latest_ready_run_id=latest_ready.run_id if latest_ready else None,
            latest_completed_run_id=latest_completed.run_id if latest_completed else None,
            latest_failed_run_id=latest_failed.run_id if latest_failed else None,
        )

    def find_run_id(self, run_id: str) -> ApiSportsRunRegistryEntry | None:
        for entry in reversed(self.entries):
            if entry.run_id == run_id:
                return entry
        return None

    def latest(
        self,
        *,
        status: str | None = None,
        ready: bool | None = None,
        quality_status: str | None = None,
    ) -> ApiSportsRunRegistryEntry | None:
        matches = self.list_entries(
            status=status,
            ready=ready,
            quality_status=quality_status,
        )
        return matches[-1] if matches else None

    def select_latest(
        self,
        *,
        status: str | None = None,
        ready: bool | None = None,
        quality_status: str | None = None,
    ) -> ApiSportsRunRegistrySelection:
        matches = self.list_entries(
            status=status,
            ready=ready,
            quality_status=quality_status,
        )
        entry = matches[-1] if matches else None

        return ApiSportsRunRegistrySelection(
            status="FOUND" if entry is not None else "NOT_FOUND",
            ledger_path=str(self.ledger_path),
            criteria={
                "status": _upper_or_none(status),
                "ready": ready,
                "quality_status": _upper_or_none(quality_status),
            },
            matches_total=len(matches),
            entry=entry,
        )

    def require_latest(
        self,
        *,
        status: str | None = None,
        ready: bool | None = None,
        quality_status: str | None = None,
    ) -> ApiSportsRunRegistryEntry:
        selection = self.select_latest(
            status=status,
            ready=ready,
            quality_status=quality_status,
        )
        if selection.entry is None:
            raise ApiSportsRunRegistryError(f"No run matches criteria: {selection.criteria}")
        return selection.entry

    def list_entries(
        self,
        *,
        status: str | None = None,
        ready: bool | None = None,
        quality_status: str | None = None,
        limit: int | None = None,
    ) -> tuple[ApiSportsRunRegistryEntry, ...]:
        status_filter = _upper_or_none(status)
        quality_filter = _upper_or_none(quality_status)

        matches = [
            entry
            for entry in self.entries
            if (status_filter is None or entry.status == status_filter)
            and (ready is None or entry.ready is ready)
            and (quality_filter is None or entry.quality_status == quality_filter)
        ]

        if limit is not None and limit >= 0:
            matches = matches[-limit:]

        return tuple(matches)


def default_run_registry_limit() -> int:
    raw = os.getenv("APISPORTS_RUN_REGISTRY_DEFAULT_LIMIT", "20")
    try:
        return max(0, int(raw))
    except ValueError as exc:
        raise ApiSportsRunRegistryError("APISPORTS_RUN_REGISTRY_DEFAULT_LIMIT must be an integer.") from exc


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsRunRegistryError(f"Ledger field is required: {field}")
    return result


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _upper(value: Any) -> str:
    if value is None:
        return ""
    enum_value = getattr(value, "value", None)
    if enum_value is not None:
        value = enum_value
    return str(value).strip().upper()


def _upper_or_none(value: Any) -> str | None:
    text = _upper(value)
    return text or None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "ready"}
    return bool(value)


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return _bool(value)


def _int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    return _int(value)
