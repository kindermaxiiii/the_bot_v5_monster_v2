from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from app.fqis.reporting.audit_history import (
    AuditHistoryReport,
    build_audit_history_report_from_bundle_root,
    build_audit_history_report_from_manifest_paths,
)


GateStatus = Literal["PASS", "WARN", "FAIL"]
HistoryDecision = Literal["ACCEPT", "REVIEW", "REJECT"]


@dataclass(slots=True, frozen=True)
class AuditHistoryGateThresholds:
    min_run_count: int = 2

    min_latest_roi_warn: float = 0.0
    min_latest_roi_fail: float = -0.10

    max_latest_brier_warn: float = 0.25
    max_latest_brier_fail: float = 0.35

    min_latest_clv_beat_rate_warn: float = 0.50
    min_latest_clv_beat_rate_fail: float = 0.40

    max_abs_latest_model_market_delta_warn: float = 0.25
    max_abs_latest_model_market_delta_fail: float = 0.50

    max_latest_model_only_count_warn: int = 0
    max_latest_model_only_count_fail: int = 10

    max_latest_clv_missing_count_warn: int = 0
    max_latest_clv_missing_count_fail: int = 5

    max_total_fail_count: int = 0
    max_total_warn_count_warn: int = 0
    max_total_warn_count_fail: int = 10

    max_health_fail_count: int = 0


@dataclass(slots=True, frozen=True)
class AuditHistoryGate:
    code: str
    status: GateStatus
    observed_value: float | int | None
    warn_threshold: float | int | None
    fail_threshold: float | int | None
    detail: str


@dataclass(slots=True, frozen=True)
class AuditHistoryGateReport:
    status: str
    decision: HistoryDecision
    run_count: int
    gate_count: int
    pass_count: int
    warn_count: int
    fail_count: int
    history_report: dict[str, Any]
    gates: tuple[AuditHistoryGate, ...]

    @property
    def is_acceptable(self) -> bool:
        return self.decision == "ACCEPT"


def evaluate_audit_history_from_bundle_root(
    bundle_root: Path,
    *,
    thresholds: AuditHistoryGateThresholds | None = None,
) -> AuditHistoryGateReport:
    history = build_audit_history_report_from_bundle_root(bundle_root)

    return evaluate_audit_history_report(
        history,
        thresholds=thresholds,
    )


def evaluate_audit_history_from_manifest_paths(
    manifest_paths: tuple[Path, ...],
    *,
    thresholds: AuditHistoryGateThresholds | None = None,
) -> AuditHistoryGateReport:
    history = build_audit_history_report_from_manifest_paths(manifest_paths)

    return evaluate_audit_history_report(
        history,
        thresholds=thresholds,
    )


def evaluate_audit_history_report(
    history: AuditHistoryReport,
    *,
    thresholds: AuditHistoryGateThresholds | None = None,
) -> AuditHistoryGateReport:
    cfg = thresholds or AuditHistoryGateThresholds()
    gates = _build_gates(history, cfg)
    decision = _decision_from_gates(gates)

    return AuditHistoryGateReport(
        status="ok",
        decision=decision,
        run_count=history.run_count,
        gate_count=len(gates),
        pass_count=sum(1 for gate in gates if gate.status == "PASS"),
        warn_count=sum(1 for gate in gates if gate.status == "WARN"),
        fail_count=sum(1 for gate in gates if gate.status == "FAIL"),
        history_report=_history_report_to_embedded_record(history),
        gates=gates,
    )


def audit_history_gate_report_to_record(report: AuditHistoryGateReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_audit_history_gate_report",
        "decision": report.decision,
        "is_acceptable": report.is_acceptable,
        "run_count": report.run_count,
        "gate_count": report.gate_count,
        "pass_count": report.pass_count,
        "warn_count": report.warn_count,
        "fail_count": report.fail_count,
        "gates": [
            _gate_to_record(gate)
            for gate in report.gates
        ],
        "history_report": dict(report.history_report),
    }


def write_audit_history_gate_report_json(
    report: AuditHistoryGateReport,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            audit_history_gate_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _build_gates(
    history: AuditHistoryReport,
    thresholds: AuditHistoryGateThresholds,
) -> tuple[AuditHistoryGate, ...]:
    latest_run = history.runs[-1] if history.runs else None
    latest_metrics = latest_run.headline_metrics if latest_run else {}

    gates = [
        _minimum_gate(
            code="MIN_RUN_COUNT",
            observed=history.run_count,
            warn_threshold=thresholds.min_run_count,
            fail_threshold=thresholds.min_run_count,
            detail=f"History must contain at least {thresholds.min_run_count} runs.",
        ),
        _maximum_gate(
            code="TOTAL_FAIL_FLAGS",
            observed=history.total_fail_count,
            warn_threshold=thresholds.max_total_fail_count,
            fail_threshold=thresholds.max_total_fail_count,
            detail="Total FAIL audit flags across history must remain within tolerance.",
        ),
        _maximum_gate(
            code="TOTAL_WARN_FLAGS",
            observed=history.total_warn_count,
            warn_threshold=thresholds.max_total_warn_count_warn,
            fail_threshold=thresholds.max_total_warn_count_fail,
            detail="Total WARN audit flags across history should remain controlled.",
        ),
        _maximum_gate(
            code="HEALTH_FAIL_COUNT",
            observed=int(history.health_counts.get("FAIL", 0)),
            warn_threshold=thresholds.max_health_fail_count,
            fail_threshold=thresholds.max_health_fail_count,
            detail="No historical run should have FAIL health status.",
        ),
        _range_low_gate(
            code="LATEST_ROI",
            observed=_optional_float(latest_metrics.get("roi")),
            warn_threshold=thresholds.min_latest_roi_warn,
            fail_threshold=thresholds.min_latest_roi_fail,
            detail="Latest ROI must not breach downside thresholds.",
        ),
        _range_high_gate(
            code="LATEST_BRIER_SCORE",
            observed=_optional_float(latest_metrics.get("brier_score")),
            warn_threshold=thresholds.max_latest_brier_warn,
            fail_threshold=thresholds.max_latest_brier_fail,
            detail="Latest Brier score must remain below calibration risk thresholds.",
        ),
        _range_low_gate(
            code="LATEST_CLV_BEAT_RATE",
            observed=_optional_float(latest_metrics.get("clv_beat_rate")),
            warn_threshold=thresholds.min_latest_clv_beat_rate_warn,
            fail_threshold=thresholds.min_latest_clv_beat_rate_fail,
            detail="Latest CLV beat rate must show that the bot beats closing prices often enough.",
        ),
        _abs_high_gate(
            code="LATEST_MODEL_MARKET_DELTA",
            observed=_optional_float(latest_metrics.get("delta_model_market_mean")),
            warn_threshold=thresholds.max_abs_latest_model_market_delta_warn,
            fail_threshold=thresholds.max_abs_latest_model_market_delta_fail,
            detail="Latest average model-market probability delta must remain controlled.",
        ),
        _maximum_gate(
            code="LATEST_MODEL_ONLY_COUNT",
            observed=_optional_int(latest_metrics.get("model_only_count")),
            warn_threshold=thresholds.max_latest_model_only_count_warn,
            fail_threshold=thresholds.max_latest_model_only_count_fail,
            detail="Latest run should minimize model-only probabilities without market prior.",
        ),
        _maximum_gate(
            code="LATEST_CLV_MISSING_COUNT",
            observed=_optional_int(latest_metrics.get("clv_missing_count")),
            warn_threshold=thresholds.max_latest_clv_missing_count_warn,
            fail_threshold=thresholds.max_latest_clv_missing_count_fail,
            detail="Latest run should not have missing closing odds.",
        ),
    ]

    return tuple(gates)


def _minimum_gate(
    *,
    code: str,
    observed: int | None,
    warn_threshold: int,
    fail_threshold: int,
    detail: str,
) -> AuditHistoryGate:
    if observed is None:
        return _gate(code, "FAIL", observed, warn_threshold, fail_threshold, f"{detail} Observed value is missing.")

    if observed < fail_threshold:
        status: GateStatus = "FAIL"
    elif observed < warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return _gate(code, status, observed, warn_threshold, fail_threshold, detail)


def _maximum_gate(
    *,
    code: str,
    observed: int | None,
    warn_threshold: int,
    fail_threshold: int,
    detail: str,
) -> AuditHistoryGate:
    if observed is None:
        return _gate(code, "FAIL", observed, warn_threshold, fail_threshold, f"{detail} Observed value is missing.")

    if observed > fail_threshold:
        status: GateStatus = "FAIL"
    elif observed > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return _gate(code, status, observed, warn_threshold, fail_threshold, detail)


def _range_low_gate(
    *,
    code: str,
    observed: float | None,
    warn_threshold: float,
    fail_threshold: float,
    detail: str,
) -> AuditHistoryGate:
    if observed is None:
        return _gate(code, "FAIL", observed, warn_threshold, fail_threshold, f"{detail} Observed value is missing.")

    if observed < fail_threshold:
        status: GateStatus = "FAIL"
    elif observed < warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return _gate(code, status, observed, warn_threshold, fail_threshold, detail)


def _range_high_gate(
    *,
    code: str,
    observed: float | None,
    warn_threshold: float,
    fail_threshold: float,
    detail: str,
) -> AuditHistoryGate:
    if observed is None:
        return _gate(code, "FAIL", observed, warn_threshold, fail_threshold, f"{detail} Observed value is missing.")

    if observed > fail_threshold:
        status: GateStatus = "FAIL"
    elif observed > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return _gate(code, status, observed, warn_threshold, fail_threshold, detail)


def _abs_high_gate(
    *,
    code: str,
    observed: float | None,
    warn_threshold: float,
    fail_threshold: float,
    detail: str,
) -> AuditHistoryGate:
    if observed is None:
        return _gate(code, "FAIL", observed, warn_threshold, fail_threshold, f"{detail} Observed value is missing.")

    abs_observed = abs(observed)

    if abs_observed > fail_threshold:
        status: GateStatus = "FAIL"
    elif abs_observed > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return _gate(code, status, observed, warn_threshold, fail_threshold, detail)


def _gate(
    code: str,
    status: GateStatus,
    observed: float | int | None,
    warn_threshold: float | int | None,
    fail_threshold: float | int | None,
    detail: str,
) -> AuditHistoryGate:
    return AuditHistoryGate(
        code=code,
        status=status,
        observed_value=observed,
        warn_threshold=warn_threshold,
        fail_threshold=fail_threshold,
        detail=detail,
    )


def _decision_from_gates(gates: tuple[AuditHistoryGate, ...]) -> HistoryDecision:
    if any(gate.status == "FAIL" for gate in gates):
        return "REJECT"

    if any(gate.status == "WARN" for gate in gates):
        return "REVIEW"

    return "ACCEPT"


def _gate_to_record(gate: AuditHistoryGate) -> dict[str, Any]:
    return {
        "code": gate.code,
        "status": gate.status,
        "observed_value": gate.observed_value,
        "warn_threshold": gate.warn_threshold,
        "fail_threshold": gate.fail_threshold,
        "detail": gate.detail,
    }


def _history_report_to_embedded_record(history: AuditHistoryReport) -> dict[str, Any]:
    from app.fqis.reporting.audit_history import audit_history_report_to_record

    return audit_history_report_to_record(history)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    return float(value)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None

    return int(value)

    