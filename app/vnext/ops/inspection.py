from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.vnext.ops.models import RuntimeCycleAuditRecord
from app.vnext.ops.reporter import build_runtime_report
from app.vnext.ops.replay import replay_runtime_export
from app.vnext.ops.runtime_cli import (
    EXIT_INSPECT_SOURCE_FAILED,
    EXIT_LATEST_RUN_MISSING,
    EXIT_PATH_UNREADABLE,
    write_json_document,
)


LATEST_RUN_INDEX_ENV_VAR = "VNEXT_LATEST_RUN_PATH"
LATEST_RUN_INDEX_PATH = Path("exports") / "vnext" / "latest_run.json"


class InspectCliError(RuntimeError):
    def __init__(self, reason: str, path: str, exit_code: int) -> None:
        super().__init__(reason)
        self.reason = reason
        self.path = path
        self.exit_code = exit_code


def resolve_latest_run_index_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    override = str(os.getenv(LATEST_RUN_INDEX_ENV_VAR) or "").strip()
    if override:
        return Path(override)
    return LATEST_RUN_INDEX_PATH


@dataclass(slots=True, frozen=True)
class RunInspectionSummary:
    timestamp_utc: str
    status: str
    source: str
    notifier: str
    cycles_requested: int | str
    cycles_executed: int | str
    preflight_status: str
    preflight_warnings: tuple[str, ...]
    preflight_errors: tuple[str, ...]
    ops_flags: tuple[str, ...]
    publishable_count: int
    retained_payload_count: int
    deduped_count: int
    unsent_shadow_count: int
    notifier_attempt_count: int
    notified_count: int
    acked_record_count: int
    manifest_path: str
    report_path: str
    export_path: str
    latest_path: str


def _load_json_object(path: Path, *, missing_reason: str, invalid_reason: str) -> dict[str, object]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise InspectCliError(missing_reason, str(path), EXIT_LATEST_RUN_MISSING if missing_reason == "latest_run_missing" else EXIT_INSPECT_SOURCE_FAILED) from exc
    except OSError as exc:
        raise InspectCliError("path_unreadable", str(path), EXIT_PATH_UNREADABLE) from exc
    try:
        payload = json.loads(raw)
    except Exception as exc:
        raise InspectCliError(invalid_reason, str(path), EXIT_INSPECT_SOURCE_FAILED) from exc
    if not isinstance(payload, dict):
        raise InspectCliError(invalid_reason, str(path), EXIT_INSPECT_SOURCE_FAILED)
    return payload


def _require_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(key)
    return value


def _optional_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _string_list(payload: dict[str, object], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(key)
    return tuple(str(item) for item in value)


def _int_value(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise ValueError(key)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(key) from exc


def _load_manifest(path: Path) -> dict[str, object]:
    payload = _load_json_object(path, missing_reason="manifest_missing", invalid_reason="manifest_invalid")
    try:
        _require_string(payload, "status")
        _require_string(payload, "source_resolved")
        _require_string(payload, "notifier_resolved")
        _require_string(payload, "export_path")
        _require_string(payload, "started_at_utc")
        _require_string(payload, "preflight_status")
        _int_value(payload, "cycles_requested")
        _int_value(payload, "cycles_executed")
        _string_list(payload, "preflight_warnings")
        _string_list(payload, "preflight_errors")
        _string_list(payload, "ops_flags")
    except ValueError as exc:
        raise InspectCliError("manifest_invalid", str(path), EXIT_INSPECT_SOURCE_FAILED) from exc
    return payload


def load_manifest_document(path: Path) -> dict[str, object]:
    return _load_manifest(path)


def _load_report(path: Path) -> dict[str, object]:
    payload = _load_json_object(path, missing_reason="report_missing", invalid_reason="report_invalid")
    try:
        _int_value(payload, "cycle_count")
        _int_value(payload, "publishable_count")
        _int_value(payload, "retained_payload_count")
        _int_value(payload, "deduped_count")
        _int_value(payload, "unsent_shadow_count")
        _int_value(payload, "notifier_attempt_count")
        _int_value(payload, "notified_count")
        _int_value(payload, "acked_record_count")
    except ValueError as exc:
        raise InspectCliError("report_invalid", str(path), EXIT_INSPECT_SOURCE_FAILED) from exc
    return payload


def load_latest_run_index(path: Path | None = None) -> dict[str, object]:
    resolved_path = resolve_latest_run_index_path(path)
    payload = _load_json_object(resolved_path, missing_reason="latest_run_missing", invalid_reason="latest_run_invalid")
    try:
        _require_string(payload, "manifest_path")
        _require_string(payload, "export_path")
        _require_string(payload, "timestamp_utc")
        _require_string(payload, "status")
        _require_string(payload, "source")
        _require_string(payload, "notifier")
        _optional_string(payload, "report_path")
    except ValueError as exc:
        raise InspectCliError("latest_run_invalid", str(resolved_path), EXIT_INSPECT_SOURCE_FAILED) from exc
    return payload


def write_latest_run_index(manifest_path: Path, manifest: dict[str, object], path: Path | None = None) -> dict[str, object]:
    resolved_path = resolve_latest_run_index_path(path)
    payload = {
        "manifest_path": str(manifest_path),
        "report_path": manifest.get("report_path") or None,
        "export_path": manifest.get("export_path"),
        "timestamp_utc": manifest.get("finished_at_utc") or manifest.get("started_at_utc"),
        "status": manifest.get("status"),
        "source": manifest.get("source_resolved"),
        "notifier": manifest.get("notifier_resolved"),
    }
    write_json_document(resolved_path, payload)
    return payload


def load_cycles_from_export(path: Path) -> tuple[RuntimeCycleAuditRecord, ...]:
    try:
        return replay_runtime_export(path)
    except FileNotFoundError as exc:
        raise InspectCliError("export_missing", str(path), EXIT_INSPECT_SOURCE_FAILED) from exc
    except OSError as exc:
        raise InspectCliError("path_unreadable", str(path), EXIT_PATH_UNREADABLE) from exc
    except ValueError as exc:
        raise InspectCliError("export_invalid", str(path), EXIT_INSPECT_SOURCE_FAILED) from exc


def _report_from_export(path: Path) -> tuple[dict[str, object], tuple[str, ...], str]:
    cycles = load_cycles_from_export(path)
    ops_flags = tuple(sorted({flag for cycle in cycles for flag in cycle.ops_flags}))
    timestamp_utc = cycles[-1].timestamp_utc.isoformat() if cycles else "-"
    return build_runtime_report(cycles), ops_flags, timestamp_utc


def _summary_from_manifest(manifest: dict[str, object], *, manifest_path: str, latest_path: str) -> RunInspectionSummary:
    export_path = _require_string(manifest, "export_path")
    report_path = _optional_string(manifest, "report_path")
    report_payload: dict[str, object] | None = None
    if report_path:
        try:
            report_payload = _load_report(Path(report_path))
        except InspectCliError as exc:
            if exc.reason not in {"report_missing", "report_invalid"}:
                raise

    if report_payload is None:
        report_payload, _, _ = _report_from_export(Path(export_path))

    finished_at = _optional_string(manifest, "finished_at_utc")
    timestamp_utc = finished_at or _require_string(manifest, "started_at_utc")
    return RunInspectionSummary(
        timestamp_utc=timestamp_utc,
        status=_require_string(manifest, "status"),
        source=_require_string(manifest, "source_resolved"),
        notifier=_require_string(manifest, "notifier_resolved"),
        cycles_requested=_int_value(manifest, "cycles_requested"),
        cycles_executed=_int_value(manifest, "cycles_executed"),
        preflight_status=_require_string(manifest, "preflight_status"),
        preflight_warnings=_string_list(manifest, "preflight_warnings"),
        preflight_errors=_string_list(manifest, "preflight_errors"),
        ops_flags=_string_list(manifest, "ops_flags"),
        publishable_count=_int_value(report_payload, "publishable_count"),
        retained_payload_count=_int_value(report_payload, "retained_payload_count"),
        deduped_count=_int_value(report_payload, "deduped_count"),
        unsent_shadow_count=_int_value(report_payload, "unsent_shadow_count"),
        notifier_attempt_count=_int_value(report_payload, "notifier_attempt_count"),
        notified_count=_int_value(report_payload, "notified_count"),
        acked_record_count=_int_value(report_payload, "acked_record_count"),
        manifest_path=manifest_path,
        report_path=report_path,
        export_path=export_path,
        latest_path=latest_path,
    )


def inspect_latest_run(path: Path | None = None) -> RunInspectionSummary:
    resolved_path = resolve_latest_run_index_path(path)
    latest = load_latest_run_index(resolved_path)
    manifest_path = Path(_require_string(latest, "manifest_path"))
    manifest = _load_manifest(manifest_path)
    return _summary_from_manifest(manifest, manifest_path=str(manifest_path), latest_path=str(resolved_path))


def inspect_manifest_path(path: Path) -> RunInspectionSummary:
    manifest = _load_manifest(path)
    return _summary_from_manifest(manifest, manifest_path=str(path), latest_path="")


def inspect_export_path(path: Path) -> RunInspectionSummary:
    report_payload, ops_flags, timestamp_utc = _report_from_export(path)
    return RunInspectionSummary(
        timestamp_utc=timestamp_utc,
        status="-",
        source="-",
        notifier="-",
        cycles_requested="-",
        cycles_executed=_int_value(report_payload, "cycle_count"),
        preflight_status="-",
        preflight_warnings=(),
        preflight_errors=(),
        ops_flags=ops_flags,
        publishable_count=_int_value(report_payload, "publishable_count"),
        retained_payload_count=_int_value(report_payload, "retained_payload_count"),
        deduped_count=_int_value(report_payload, "deduped_count"),
        unsent_shadow_count=_int_value(report_payload, "unsent_shadow_count"),
        notifier_attempt_count=_int_value(report_payload, "notifier_attempt_count"),
        notified_count=_int_value(report_payload, "notified_count"),
        acked_record_count=_int_value(report_payload, "acked_record_count"),
        manifest_path="",
        report_path="",
        export_path=str(path),
        latest_path="",
    )


def format_run_inspection(summary: RunInspectionSummary) -> str:
    return "\n".join(
        (
            "vnext_run_inspect "
            f"timestamp_utc={summary.timestamp_utc} "
            f"status={summary.status} "
            f"source={summary.source} "
            f"notifier={summary.notifier} "
            f"cycles_requested={summary.cycles_requested} "
            f"cycles_executed={summary.cycles_executed} "
            f"preflight={summary.preflight_status}",
            "vnext_run_inspect_counts "
            f"publishable={summary.publishable_count} "
            f"retained={summary.retained_payload_count} "
            f"deduped={summary.deduped_count} "
            f"shadow_unsent={summary.unsent_shadow_count} "
            f"notify_attempts={summary.notifier_attempt_count} "
            f"notified={summary.notified_count} "
            f"acked_records={summary.acked_record_count}",
            "vnext_run_inspect_flags "
            f"ops_flags={list(summary.ops_flags)} "
            f"preflight_warnings={list(summary.preflight_warnings)} "
            f"preflight_errors={list(summary.preflight_errors)}",
            "vnext_run_inspect_paths "
            f"latest={summary.latest_path or '-'} "
            f"manifest={summary.manifest_path or '-'} "
            f"report={summary.report_path or '-'} "
            f"export={summary.export_path or '-'}",
        )
    )
