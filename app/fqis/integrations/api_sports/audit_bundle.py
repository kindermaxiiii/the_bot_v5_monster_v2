
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.run_ledger import (
    ApiSportsRunLedgerError,
    build_run_ledger_entry,
)
from app.fqis.integrations.api_sports.run_registry import (
    ApiSportsRunRegistry,
    ApiSportsRunRegistryError,
)


@dataclass(frozen=True)
class ApiSportsAuditBundleFile:
    role: str
    path: str
    exists: bool
    sha256: str | None
    size_bytes: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "path": self.path,
            "exists": self.exists,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class ApiSportsAuditBundle:
    status: str
    run_id: str
    ready: bool
    created_at_utc: str
    manifest_path: str
    output_path: str | None
    files: tuple[ApiSportsAuditBundleFile, ...]
    ledger_entry: Mapping[str, Any]
    manifest: Mapping[str, Any]
    quality_report: Mapping[str, Any] | None
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "ready": self.ready,
            "created_at_utc": self.created_at_utc,
            "manifest_path": self.manifest_path,
            "output_path": self.output_path,
            "files": [item.to_dict() for item in self.files],
            "ledger_entry": dict(self.ledger_entry),
            "manifest": dict(self.manifest),
            "quality_report": dict(self.quality_report) if self.quality_report is not None else None,
            "errors": list(self.errors),
        }


class ApiSportsAuditBundleError(RuntimeError):
    pass


def build_api_sports_audit_bundle(manifest_path: str | Path) -> ApiSportsAuditBundle:
    path = Path(manifest_path)
    manifest = _load_json_object(path)

    try:
        ledger_entry = build_run_ledger_entry(path)
    except ApiSportsRunLedgerError as exc:
        raise ApiSportsAuditBundleError(str(exc)) from exc

    run_dir = _optional_str(manifest.get("run_dir"))
    quality_report_path = _find_quality_report(path, run_dir)
    quality_report = _load_json_object(quality_report_path) if quality_report_path is not None else None

    files: list[ApiSportsAuditBundleFile] = [
        _file_record("pipeline_manifest", path),
    ]

    if quality_report_path is not None:
        files.append(_file_record("quality_report", quality_report_path))

    normalized_input = _optional_str(manifest.get("normalized_input"))
    if normalized_input is not None:
        files.append(_file_record("normalized_input", Path(normalized_input)))

    errors = tuple(_bundle_errors(ledger_entry.to_dict(), files, quality_report))

    return ApiSportsAuditBundle(
        status="BUILT",
        run_id=ledger_entry.run_id,
        ready=ledger_entry.ready and not errors,
        created_at_utc=_utc_now(),
        manifest_path=str(path),
        output_path=None,
        files=tuple(files),
        ledger_entry=ledger_entry.to_dict(),
        manifest=manifest,
        quality_report=quality_report,
        errors=errors,
    )


def write_api_sports_audit_bundle(
    manifest_path: str | Path,
    *,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> ApiSportsAuditBundle:
    bundle = build_api_sports_audit_bundle(manifest_path)

    target = (
        Path(output_path)
        if output_path is not None
        else Path(output_dir or default_audit_bundle_dir()) / f"{_safe_run_id(bundle.run_id)}_audit_bundle.json"
    )

    final_bundle = replace(bundle, output_path=str(target))
    _write_json_atomic(target, final_bundle.to_dict())
    return final_bundle


def resolve_manifest_from_registry(
    *,
    ledger_path: str | Path | None = None,
    run_id: str | None = None,
    latest_ready: bool = False,
    latest_completed: bool = False,
) -> Path:
    registry = ApiSportsRunRegistry(ledger_path)

    if run_id:
        entry = registry.find_run_id(run_id)
    elif latest_ready:
        entry = registry.latest(ready=True)
    elif latest_completed:
        entry = registry.latest(status="COMPLETED")
    else:
        entry = registry.latest()

    if entry is None:
        raise ApiSportsAuditBundleError("No run found in registry for requested criteria.")
    if not entry.manifest_path:
        raise ApiSportsAuditBundleError(f"Run has no manifest_path: {entry.run_id}")

    return Path(entry.manifest_path)


def default_audit_bundle_dir() -> Path:
    return Path(os.getenv("APISPORTS_AUDIT_BUNDLE_DIR", "data/pipeline/api_sports/audit_bundles"))


def _bundle_errors(
    ledger_entry: Mapping[str, Any],
    files: tuple[ApiSportsAuditBundleFile, ...] | list[ApiSportsAuditBundleFile],
    quality_report: Mapping[str, Any] | None,
) -> list[str]:
    errors: list[str] = []

    if _upper(ledger_entry.get("status")) == "FAILED":
        errors.append("PIPELINE_STATUS_FAILED")

    for item in files:
        if not item.exists:
            errors.append(f"MISSING_FILE:{item.role}:{item.path}")

    if quality_report is None:
        errors.append("MISSING_QUALITY_REPORT")
    elif _upper(quality_report.get("status")) == "BLOCKED":
        errors.append("QUALITY_STATUS_BLOCKED")

    return errors


def _find_quality_report(manifest_path: Path, run_dir: str | None) -> Path | None:
    candidates: list[Path] = []

    if run_dir:
        candidates.append(Path(run_dir) / "quality_report.json")

    candidates.append(manifest_path.parent / "quality_report.json")

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)

        if candidate.exists() and candidate.is_file():
            return candidate

    return None


def _file_record(role: str, path: Path) -> ApiSportsAuditBundleFile:
    exists = path.exists() and path.is_file()
    raw_bytes = path.read_bytes() if exists else None

    return ApiSportsAuditBundleFile(
        role=role,
        path=str(path),
        exists=exists,
        sha256=hashlib.sha256(raw_bytes).hexdigest() if raw_bytes is not None else None,
        size_bytes=len(raw_bytes) if raw_bytes is not None else None,
    )


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApiSportsAuditBundleError(f"JSON path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsAuditBundleError(f"JSON path is not a file: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ApiSportsAuditBundleError(f"JSON path is invalid: {path}") from exc

    if not isinstance(payload, dict):
        raise ApiSportsAuditBundleError(f"JSON path must contain an object: {path}")

    return payload


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _safe_run_id(run_id: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in run_id)


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
