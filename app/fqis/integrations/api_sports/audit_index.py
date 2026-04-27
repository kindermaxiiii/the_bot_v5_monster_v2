
from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.audit_bundle import default_audit_bundle_dir


@dataclass(frozen=True)
class ApiSportsAuditIndexEntry:
    run_id: str
    status: str
    ready: bool
    bundle_path: str
    bundle_sha256: str
    created_at_utc: str | None
    output_path: str | None
    manifest_path: str | None
    pipeline_status: str | None
    quality_status: str | None
    files_total: int
    files_present: int
    files_missing: int
    errors_total: int

    @classmethod
    def from_file(cls, path: str | Path) -> "ApiSportsAuditIndexEntry":
        bundle_path = Path(path)
        if not bundle_path.exists():
            raise ApiSportsAuditIndexError(f"Bundle path does not exist: {bundle_path}")
        if not bundle_path.is_file():
            raise ApiSportsAuditIndexError(f"Bundle path is not a file: {bundle_path}")

        raw_bytes = bundle_path.read_bytes()
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiSportsAuditIndexError(f"Bundle is not valid JSON: {bundle_path}") from exc

        if not isinstance(payload, Mapping):
            raise ApiSportsAuditIndexError(f"Bundle must be a JSON object: {bundle_path}")

        files = _records(payload.get("files"))
        errors = _sequence(payload.get("errors"))

        quality_report = payload.get("quality_report")
        manifest = payload.get("manifest")

        return cls(
            run_id=_required_str(payload.get("run_id"), "run_id"),
            status=_upper(_required_str(payload.get("status"), "status")),
            ready=_bool(payload.get("ready")),
            bundle_path=str(bundle_path),
            bundle_sha256=hashlib.sha256(raw_bytes).hexdigest(),
            created_at_utc=_optional_str(payload.get("created_at_utc")),
            output_path=_optional_str(payload.get("output_path")),
            manifest_path=_optional_str(payload.get("manifest_path")),
            pipeline_status=_nested_status(manifest),
            quality_status=_nested_status(quality_report),
            files_total=len(files),
            files_present=sum(1 for item in files if _bool(item.get("exists"))),
            files_missing=sum(1 for item in files if not _bool(item.get("exists"))),
            errors_total=len(errors),
        )

    @property
    def bundle_key(self) -> str:
        return f"{self.run_id}:{self.bundle_sha256}"

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "ready": self.ready,
            "bundle_path": self.bundle_path,
            "bundle_sha256": self.bundle_sha256,
            "bundle_key": self.bundle_key,
            "created_at_utc": self.created_at_utc,
            "output_path": self.output_path,
            "manifest_path": self.manifest_path,
            "pipeline_status": self.pipeline_status,
            "quality_status": self.quality_status,
            "files_total": self.files_total,
            "files_present": self.files_present,
            "files_missing": self.files_missing,
            "errors_total": self.errors_total,
        }


@dataclass(frozen=True)
class ApiSportsAuditIndex:
    status: str
    bundle_dir: str
    index_path: str | None
    created_at_utc: str
    bundles_total: int
    ready_total: int
    status_counts: Mapping[str, int]
    quality_status_counts: Mapping[str, int]
    latest_run_id: str | None
    latest_ready_run_id: str | None
    entries: tuple[ApiSportsAuditIndexEntry, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "bundle_dir": self.bundle_dir,
            "index_path": self.index_path,
            "created_at_utc": self.created_at_utc,
            "bundles_total": self.bundles_total,
            "ready_total": self.ready_total,
            "status_counts": dict(self.status_counts),
            "quality_status_counts": dict(self.quality_status_counts),
            "latest_run_id": self.latest_run_id,
            "latest_ready_run_id": self.latest_ready_run_id,
            "entries": [entry.to_dict() for entry in self.entries],
            "errors": list(self.errors),
        }


class ApiSportsAuditIndexError(RuntimeError):
    pass


def build_api_sports_audit_index(
    *,
    bundle_dir: str | Path | None = None,
    index_path: str | Path | None = None,
    pattern: str = "*.json",
) -> ApiSportsAuditIndex:
    bundle_dir_path = Path(bundle_dir) if bundle_dir is not None else default_audit_bundle_dir()
    errors: list[str] = []
    entries: list[ApiSportsAuditIndexEntry] = []

    if not bundle_dir_path.exists():
        errors.append(f"BUNDLE_DIR_NOT_FOUND:{bundle_dir_path}")
    elif not bundle_dir_path.is_dir():
        errors.append(f"BUNDLE_DIR_NOT_DIRECTORY:{bundle_dir_path}")
    else:
        for path in sorted(bundle_dir_path.glob(pattern)):
            if not path.is_file() or _is_index_file(path):
                continue
            try:
                entries.append(ApiSportsAuditIndexEntry.from_file(path))
            except ApiSportsAuditIndexError as exc:
                errors.append(str(exc))

    sorted_entries = tuple(
        sorted(
            entries,
            key=lambda entry: (
                entry.created_at_utc or "",
                entry.run_id,
                entry.bundle_path,
            ),
        )
    )

    latest = sorted_entries[-1] if sorted_entries else None
    latest_ready = next((entry for entry in reversed(sorted_entries) if entry.ready), None)

    return ApiSportsAuditIndex(
        status="BUILT" if not errors else "BUILT_WITH_ERRORS",
        bundle_dir=str(bundle_dir_path),
        index_path=str(index_path) if index_path is not None else None,
        created_at_utc=_utc_now(),
        bundles_total=len(sorted_entries),
        ready_total=sum(1 for entry in sorted_entries if entry.ready),
        status_counts=dict(Counter(entry.status for entry in sorted_entries)),
        quality_status_counts=dict(Counter(entry.quality_status for entry in sorted_entries if entry.quality_status)),
        latest_run_id=latest.run_id if latest else None,
        latest_ready_run_id=latest_ready.run_id if latest_ready else None,
        entries=sorted_entries,
        errors=tuple(errors),
    )


def write_api_sports_audit_index(
    *,
    bundle_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> ApiSportsAuditIndex:
    target = Path(output_path) if output_path is not None else default_audit_index_path()
    index = build_api_sports_audit_index(bundle_dir=bundle_dir, index_path=target)
    _write_json_atomic(target, index.to_dict())
    return index


def select_latest_audit_bundle(
    *,
    bundle_dir: str | Path | None = None,
    ready: bool | None = None,
    status: str | None = None,
    quality_status: str | None = None,
) -> ApiSportsAuditIndexEntry | None:
    index = build_api_sports_audit_index(bundle_dir=bundle_dir)
    status_filter = _upper_or_none(status)
    quality_filter = _upper_or_none(quality_status)

    matches = [
        entry
        for entry in index.entries
        if (ready is None or entry.ready is ready)
        and (status_filter is None or entry.status == status_filter)
        and (quality_filter is None or entry.quality_status == quality_filter)
    ]

    return matches[-1] if matches else None


def default_audit_index_path() -> Path:
    return Path(
        os.getenv(
            "APISPORTS_AUDIT_INDEX_PATH",
            str(default_audit_bundle_dir() / "audit_bundle_index.json"),
        )
    )


def _is_index_file(path: Path) -> bool:
    return path.name == "audit_bundle_index.json"


def _records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return value


def _nested_status(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    return _upper_or_none(value.get("status"))


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsAuditIndexError(f"Bundle field is required: {field}")
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


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
