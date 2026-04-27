from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from app.fqis.reporting.audit_gates import (
    AuditHistoryGate,
    AuditHistoryGateReport,
    AuditHistoryGateThresholds,
    audit_history_gate_report_to_record,
    evaluate_audit_history_from_bundle_root,
    evaluate_audit_history_from_manifest_paths,
)


ReadinessStatus = Literal["GO", "NO_GO"]
ReadinessLevel = Literal["READY", "REVIEW_REQUIRED", "BLOCKED"]


@dataclass(slots=True, frozen=True)
class ReadinessChecklistItem:
    code: str
    status: str
    blocking: bool
    detail: str
    recommended_action: str


@dataclass(slots=True, frozen=True)
class ProductionReadinessReport:
    status: str
    readiness_status: ReadinessStatus
    readiness_level: ReadinessLevel
    gate_decision: str
    run_count: int
    checklist_count: int
    blocker_count: int
    warning_count: int
    failure_count: int
    recommended_actions: tuple[str, ...]
    checklist: tuple[ReadinessChecklistItem, ...]
    gate_report: dict[str, Any]

    @property
    def is_go(self) -> bool:
        return self.readiness_status == "GO"


def evaluate_production_readiness_from_bundle_root(
    bundle_root: Path,
    *,
    thresholds: AuditHistoryGateThresholds | None = None,
) -> ProductionReadinessReport:
    gate_report = evaluate_audit_history_from_bundle_root(
        bundle_root,
        thresholds=thresholds,
    )

    return evaluate_production_readiness_from_gate_report(gate_report)


def evaluate_production_readiness_from_manifest_paths(
    manifest_paths: tuple[Path, ...],
    *,
    thresholds: AuditHistoryGateThresholds | None = None,
) -> ProductionReadinessReport:
    gate_report = evaluate_audit_history_from_manifest_paths(
        manifest_paths,
        thresholds=thresholds,
    )

    return evaluate_production_readiness_from_gate_report(gate_report)


def evaluate_production_readiness_from_gate_report(
    gate_report: AuditHistoryGateReport,
) -> ProductionReadinessReport:
    checklist = tuple(_gate_to_checklist_item(gate) for gate in gate_report.gates)
    blockers = tuple(item for item in checklist if item.blocking)
    recommended_actions = _dedupe_actions(item.recommended_action for item in blockers)

    readiness_status, readiness_level = _readiness_from_gate_decision(gate_report.decision)

    return ProductionReadinessReport(
        status="ok",
        readiness_status=readiness_status,
        readiness_level=readiness_level,
        gate_decision=gate_report.decision,
        run_count=gate_report.run_count,
        checklist_count=len(checklist),
        blocker_count=len(blockers),
        warning_count=sum(1 for item in checklist if item.status == "WARN"),
        failure_count=sum(1 for item in checklist if item.status == "FAIL"),
        recommended_actions=recommended_actions,
        checklist=checklist,
        gate_report=audit_history_gate_report_to_record(gate_report),
    )


def production_readiness_report_to_record(report: ProductionReadinessReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_production_readiness_report",
        "readiness_status": report.readiness_status,
        "readiness_level": report.readiness_level,
        "is_go": report.is_go,
        "gate_decision": report.gate_decision,
        "run_count": report.run_count,
        "checklist_count": report.checklist_count,
        "blocker_count": report.blocker_count,
        "warning_count": report.warning_count,
        "failure_count": report.failure_count,
        "recommended_actions": list(report.recommended_actions),
        "checklist": [
            _checklist_item_to_record(item)
            for item in report.checklist
        ],
        "gate_report": dict(report.gate_report),
    }


def write_production_readiness_report_json(
    report: ProductionReadinessReport,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            production_readiness_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _readiness_from_gate_decision(decision: str) -> tuple[ReadinessStatus, ReadinessLevel]:
    if decision == "ACCEPT":
        return "GO", "READY"

    if decision == "REJECT":
        return "NO_GO", "BLOCKED"

    return "NO_GO", "REVIEW_REQUIRED"


def _gate_to_checklist_item(gate: AuditHistoryGate) -> ReadinessChecklistItem:
    blocking = gate.status in {"WARN", "FAIL"}

    return ReadinessChecklistItem(
        code=gate.code,
        status=gate.status,
        blocking=blocking,
        detail=gate.detail,
        recommended_action=_recommended_action_for_gate(gate),
    )


def _recommended_action_for_gate(gate: AuditHistoryGate) -> str:
    if gate.status == "PASS":
        return "No action required."

    actions = {
        "MIN_RUN_COUNT": "Accumulate more audited shadow runs before production-shadow approval.",
        "TOTAL_FAIL_FLAGS": "Investigate all FAIL audit flags before continuing.",
        "TOTAL_WARN_FLAGS": "Review recurring WARN flags and document mitigation before launch.",
        "HEALTH_FAIL_COUNT": "Block launch until no historical run has FAIL health status.",
        "LATEST_ROI": "Continue shadow tracking; do not promote if latest ROI breaches downside limits.",
        "LATEST_BRIER_SCORE": "Review calibration quality and probability model reliability.",
        "LATEST_CLV_BEAT_RATE": "Improve price selection and market timing before launch.",
        "LATEST_MODEL_MARKET_DELTA": "Investigate large model-market disagreement and recalibrate hybrid weighting.",
        "LATEST_MODEL_ONLY_COUNT": "Improve market-prior coverage so fewer probabilities rely on model-only fallback.",
        "LATEST_CLV_MISSING_COUNT": "Improve closing odds capture before production-shadow approval.",
    }

    return actions.get(gate.code, "Review this gate before production-shadow approval.")


def _dedupe_actions(actions) -> tuple[str, ...]:
    deduped: list[str] = []

    for action in actions:
        if action != "No action required." and action not in deduped:
            deduped.append(action)

    return tuple(deduped)


def _checklist_item_to_record(item: ReadinessChecklistItem) -> dict[str, Any]:
    return {
        "code": item.code,
        "status": item.status,
        "blocking": item.blocking,
        "detail": item.detail,
        "recommended_action": item.recommended_action,
    }