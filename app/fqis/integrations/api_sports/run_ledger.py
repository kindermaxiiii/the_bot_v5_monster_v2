
from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ApiSportsRunLedgerEntry:
    run_id: str
    status: str
    ready: bool
    run_dir: str | None
    manifest_path: str
    manifest_sha256: str
    normalized_input: str | None
    payload_sha256: str | None
    started_at_utc: str | None
    completed_at_utc: str | None
    steps_total: int
    steps_completed: int
    steps_failed: int
    errors_total: int
    quality_status: str | None = None
    quality_ready: bool | None = None
    quality_issues_total: int | None = None

    @property
    def ledger_key(self) -> str:
        return f"{self.run_id}:{self.manifest_sha256}"

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "ready": self.ready,
            "run_dir": self.run_dir,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
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
            "ledger_key": self.ledger_key,
        }


@dataclass(frozen=True)
class ApiSportsRunLedgerSummary:
    ledger_path: str
    runs_total: int
    runs_ready: int
    status_counts: Mapping[str, int]
    quality_status_counts: Mapping[str, int]
    latest_run_id: str | None
    latest_status: str | None
    latest_ready: bool | None

    def to_dict(self) -> dict[str, object]:
        return {
            "ledger_path": self.ledger_path,
            "runs_total": self.runs_total,
            "runs_ready": self.runs_ready,
            "status_counts": dict(self.status_counts),
            "quality_status_counts": dict(self.quality_status_counts),
            "latest_run_id": self.latest_run_id,
            "latest_status": self.latest_status,
            "latest_ready": self.latest_ready,
        }


class ApiSportsRunLedgerError(RuntimeError):
    pass


def build_run_ledger_entry(manifest_path: str | Path) -> ApiSportsRunLedgerEntry:
    path = Path(manifest_path)
    if not path.exists():
        raise ApiSportsRunLedgerError(f"Manifest path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsRunLedgerError(f"Manifest path is not a file: {path}")

    raw_bytes = path.read_bytes()
    try:
        manifest = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ApiSportsRunLedgerError(f"Manifest is not valid JSON: {path}") from exc

    if not isinstance(manifest, Mapping):
        raise ApiSportsRunLedgerError("Manifest payload must be a JSON object.")

    steps = _records(manifest.get("steps"))
    errors = _sequence(manifest.get("errors"))

    run_dir = _optional_str(manifest.get("run_dir"))
    quality = _read_quality_report(path, run_dir)

    return ApiSportsRunLedgerEntry(
        run_id=_required_str(manifest.get("run_id"), "run_id"),
        status=_required_str(manifest.get("status"), "status"),
        ready=bool(manifest.get("ready")),
        run_dir=run_dir,
        manifest_path=str(path),
        manifest_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        normalized_input=_optional_str(manifest.get("normalized_input")),
        payload_sha256=_optional_str(manifest.get("payload_sha256")),
        started_at_utc=_optional_str(manifest.get("started_at_utc")),
        completed_at_utc=_optional_str(manifest.get("completed_at_utc")),
        steps_total=len(steps),
        steps_completed=sum(1 for step in steps if _upper(step.get("status")) == "COMPLETED"),
        steps_failed=sum(1 for step in steps if _upper(step.get("status")) == "FAILED"),
        errors_total=len(errors),
        quality_status=_optional_str(quality.get("status")) if quality else None,
        quality_ready=bool(quality.get("ready")) if quality and "ready" in quality else None,
        quality_issues_total=len(_sequence(quality.get("issues"))) if quality else None,
    )


def append_run_ledger_entry(
    ledger_path: str | Path,
    entry: ApiSportsRunLedgerEntry,
    *,
    dedupe: bool = True,
) -> bool:
    path = Path(ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if dedupe:
        existing_keys = {item.get("ledger_key") for item in read_run_ledger(path)}
        if entry.ledger_key in existing_keys:
            return False

    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(entry.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")
    return True


def record_pipeline_manifest(
    manifest_path: str | Path,
    *,
    ledger_path: str | Path | None = None,
    dedupe: bool = True,
) -> ApiSportsRunLedgerEntry:
    entry = build_run_ledger_entry(manifest_path)
    append_run_ledger_entry(
        ledger_path or default_run_ledger_path(),
        entry,
        dedupe=dedupe,
    )
    return entry


def read_run_ledger(ledger_path: str | Path) -> list[dict[str, Any]]:
    path = Path(ledger_path)
    if not path.exists():
        return []

    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ApiSportsRunLedgerError(f"Invalid JSONL at {path}:{line_number}") from exc
        if not isinstance(payload, dict):
            raise ApiSportsRunLedgerError(f"Ledger line must be a JSON object at {path}:{line_number}")
        records.append(payload)
    return records


def summarize_run_ledger(ledger_path: str | Path) -> ApiSportsRunLedgerSummary:
    path = Path(ledger_path)
    records = read_run_ledger(path)

    status_counts = Counter(_upper(record.get("status")) for record in records if record.get("status") is not None)
    quality_status_counts = Counter(
        _upper(record.get("quality_status"))
        for record in records
        if record.get("quality_status") is not None
    )

    latest = records[-1] if records else None

    return ApiSportsRunLedgerSummary(
        ledger_path=str(path),
        runs_total=len(records),
        runs_ready=sum(1 for record in records if bool(record.get("ready"))),
        status_counts=dict(status_counts),
        quality_status_counts=dict(quality_status_counts),
        latest_run_id=_optional_str(latest.get("run_id")) if latest else None,
        latest_status=_optional_str(latest.get("status")) if latest else None,
        latest_ready=bool(latest.get("ready")) if latest else None,
    )


def default_run_ledger_path() -> Path:
    return Path(os.getenv("APISPORTS_RUN_LEDGER_PATH", "data/pipeline/api_sports/run_ledger.jsonl"))


def _read_quality_report(manifest_path: Path, run_dir: str | None) -> Mapping[str, Any] | None:
    candidates: list[Path] = []

    if run_dir:
        candidates.append(Path(run_dir) / "quality_report.json")

    candidates.append(manifest_path.parent / "quality_report.json")

    for path in candidates:
        if not path.exists() or not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            return payload
    return None


def _records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return value


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsRunLedgerError(f"Manifest field is required: {field}")
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
