from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from app.fqis.config.profiles import (
    ShadowProductionProfile,
    load_shadow_production_profile,
    shadow_production_profile_to_record,
)


ChecklistStatus = Literal["PASS", "WARN", "FAIL"]
OperatorReadiness = Literal["READY", "BLOCKED"]


@dataclass(slots=True, frozen=True)
class Level1ChecklistItem:
    code: str
    status: ChecklistStatus
    blocking: bool
    detail: str
    recommended_action: str


@dataclass(slots=True, frozen=True)
class Level1OperatorChecklistReport:
    status: str
    source: str
    generated_at_utc: str
    profile_name: str
    readiness: OperatorReadiness
    item_count: int
    pass_count: int
    warn_count: int
    fail_count: int
    blocking_count: int
    profile: dict[str, Any] | None
    items: tuple[Level1ChecklistItem, ...]

    @property
    def is_ready(self) -> bool:
        return self.readiness == "READY"


def build_level1_operator_checklist(
    *,
    profile_name: str = "demo",
    profile_path: Path | None = None,
    latest_status_path: Path | None = None,
    shadow_script_path: Path = Path("scripts/fqis_shadow.py"),
    runbook_path: Path = Path("docs/fqis/LEVEL1_OPERATOR_RUNBOOK.md"),
    launch_checklist_path: Path = Path("docs/fqis/LEVEL1_LAUNCH_CHECKLIST.md"),
) -> Level1OperatorChecklistReport:
    generated_at_utc = datetime.now(UTC).isoformat()
    items: list[Level1ChecklistItem] = []
    profile: ShadowProductionProfile | None = None
    profile_record: dict[str, Any] | None = None

    try:
        profile = load_shadow_production_profile(
            profile_name=profile_name,
            profile_path=profile_path,
        )
        profile_record = shadow_production_profile_to_record(profile)
        items.append(
            Level1ChecklistItem(
                code="PROFILE_LOAD",
                status="PASS",
                blocking=False,
                detail=f"Profile loaded: {profile_name}",
                recommended_action="No action required.",
            )
        )
    except Exception as exc:  # noqa: BLE001 - operator checklist must capture config failures.
        items.append(
            Level1ChecklistItem(
                code="PROFILE_LOAD",
                status="FAIL",
                blocking=True,
                detail=f"Profile could not be loaded: {exc}",
                recommended_action="Fix the profile name, profile file, or environment overrides.",
            )
        )

    items.append(
        _file_exists_item(
            code="SHADOW_SCRIPT_PRESENT",
            path=shadow_script_path,
            required=True,
            detail_if_present="One-command shadow runner script is present.",
            action_if_missing="Restore scripts/fqis_shadow.py before launching Niveau 1.",
        )
    )

    items.append(
        _file_exists_item(
            code="RUNBOOK_PRESENT",
            path=runbook_path,
            required=True,
            detail_if_present="Operator runbook is present.",
            action_if_missing="Restore docs/fqis/LEVEL1_OPERATOR_RUNBOOK.md.",
        )
    )

    items.append(
        _file_exists_item(
            code="LAUNCH_CHECKLIST_PRESENT",
            path=launch_checklist_path,
            required=True,
            detail_if_present="Launch checklist is present.",
            action_if_missing="Restore docs/fqis/LEVEL1_LAUNCH_CHECKLIST.md.",
        )
    )

    if profile is not None:
        items.extend(
            [
                _file_exists_item(
                    code="INPUT_PATH_EXISTS",
                    path=profile.input_path,
                    required=True,
                    detail_if_present=f"Input file exists: {profile.input_path}",
                    action_if_missing="Create or refresh the input JSONL before launching shadow production.",
                ),
                _file_exists_item(
                    code="RESULTS_PATH_EXISTS",
                    path=profile.results_path,
                    required=True,
                    detail_if_present=f"Results file exists: {profile.results_path}",
                    action_if_missing="Provide the match results JSONL required for settlement.",
                ),
                _file_exists_item(
                    code="CLOSING_PATH_EXISTS",
                    path=profile.closing_path,
                    required=True,
                    detail_if_present=f"Closing odds file exists: {profile.closing_path}",
                    action_if_missing="Provide closing odds JSONL required for CLV and audit.",
                ),
                _directory_configured_item(
                    code="OUTPUT_ROOT_CONFIGURED",
                    path=profile.output_root,
                    detail_if_present=f"Output root exists: {profile.output_root}",
                    detail_if_missing=f"Output root does not exist yet and will be created by the runner: {profile.output_root}",
                ),
                _directory_configured_item(
                    code="AUDIT_HISTORY_ROOT_CONFIGURED",
                    path=profile.audit_bundle_root,
                    detail_if_present=f"Audit history root exists: {profile.audit_bundle_root}",
                    detail_if_missing=(
                        "Audit history root does not exist yet and will be created by the runner: "
                        f"{profile.audit_bundle_root}"
                    ),
                ),
                _latest_status_item(
                    path=latest_status_path or profile.output_root / "latest_status.json",
                ),
            ]
        )

    items.append(
        Level1ChecklistItem(
            code="SHADOW_ONLY_POLICY",
            status="PASS",
            blocking=False,
            detail="Niveau 1 is shadow-only: no real-money staking or automated betting is authorized.",
            recommended_action="Use Niveau 1 only for observation, CLV tracking, settlement, and audit.",
        )
    )

    fail_count = sum(1 for item in items if item.status == "FAIL")
    warn_count = sum(1 for item in items if item.status == "WARN")
    pass_count = sum(1 for item in items if item.status == "PASS")
    blocking_count = sum(1 for item in items if item.blocking)

    return Level1OperatorChecklistReport(
        status="ok",
        source="fqis_level1_operator_checklist",
        generated_at_utc=generated_at_utc,
        profile_name=profile_name,
        readiness="BLOCKED" if fail_count else "READY",
        item_count=len(items),
        pass_count=pass_count,
        warn_count=warn_count,
        fail_count=fail_count,
        blocking_count=blocking_count,
        profile=profile_record,
        items=tuple(items),
    )


def level1_operator_checklist_to_record(
    report: Level1OperatorChecklistReport,
) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": report.source,
        "generated_at_utc": report.generated_at_utc,
        "profile_name": report.profile_name,
        "readiness": report.readiness,
        "is_ready": report.is_ready,
        "item_count": report.item_count,
        "pass_count": report.pass_count,
        "warn_count": report.warn_count,
        "fail_count": report.fail_count,
        "blocking_count": report.blocking_count,
        "profile": dict(report.profile) if report.profile else None,
        "items": [_item_to_record(item) for item in report.items],
    }


def write_level1_operator_checklist_json(
    report: Level1OperatorChecklistReport,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            level1_operator_checklist_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _file_exists_item(
    *,
    code: str,
    path: Path,
    required: bool,
    detail_if_present: str,
    action_if_missing: str,
) -> Level1ChecklistItem:
    if path.exists():
        return Level1ChecklistItem(
            code=code,
            status="PASS",
            blocking=False,
            detail=detail_if_present,
            recommended_action="No action required.",
        )

    return Level1ChecklistItem(
        code=code,
        status="FAIL" if required else "WARN",
        blocking=required,
        detail=f"Missing path: {path}",
        recommended_action=action_if_missing,
    )


def _directory_configured_item(
    *,
    code: str,
    path: Path,
    detail_if_present: str,
    detail_if_missing: str,
) -> Level1ChecklistItem:
    if path.exists():
        return Level1ChecklistItem(
            code=code,
            status="PASS",
            blocking=False,
            detail=detail_if_present,
            recommended_action="No action required.",
        )

    return Level1ChecklistItem(
        code=code,
        status="WARN",
        blocking=False,
        detail=detail_if_missing,
        recommended_action="No action required if the runner has permission to create this directory.",
    )


def _latest_status_item(path: Path) -> Level1ChecklistItem:
    if not path.exists():
        return Level1ChecklistItem(
            code="LATEST_STATUS_PRESENT",
            status="WARN",
            blocking=False,
            detail=f"No latest status file found yet: {path}",
            recommended_action="Run scripts/fqis_shadow.py once to create latest_status.json.",
        )

    try:
        record = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        return Level1ChecklistItem(
            code="LATEST_STATUS_PRESENT",
            status="FAIL",
            blocking=True,
            detail=f"latest_status.json is not valid JSON: {exc}",
            recommended_action="Delete or regenerate latest_status.json by rerunning the shadow runner.",
        )

    event_type = str(record.get("event_type", "UNKNOWN"))
    status = str(record.get("status", "UNKNOWN"))

    if event_type == "FAILED" or status == "failed":
        error = record.get("error") or {}
        return Level1ChecklistItem(
            code="LATEST_STATUS_PRESENT",
            status="FAIL",
            blocking=True,
            detail=f"Latest shadow run failed: {error.get('error_type', 'UNKNOWN')}",
            recommended_action="Inspect latest_status.json and run_events.jsonl before launching again.",
        )

    if event_type == "COMPLETED" and status == "ok":
        return Level1ChecklistItem(
            code="LATEST_STATUS_PRESENT",
            status="PASS",
            blocking=False,
            detail=f"Latest shadow status is healthy: {path}",
            recommended_action="No action required.",
        )

    return Level1ChecklistItem(
        code="LATEST_STATUS_PRESENT",
        status="WARN",
        blocking=False,
        detail=f"Latest shadow status is not final: event_type={event_type} status={status}",
        recommended_action="Check whether a run is still active or rerun the shadow command.",
    )


def _item_to_record(item: Level1ChecklistItem) -> dict[str, Any]:
    return {
        "code": item.code,
        "status": item.status,
        "blocking": item.blocking,
        "detail": item.detail,
        "recommended_action": item.recommended_action,
    }