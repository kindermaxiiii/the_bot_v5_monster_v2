from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from app.fqis.performance.clv import (
    build_clv_report_from_json,
    clv_report_to_record,
)
from app.fqis.performance.metrics import (
    build_performance_report_from_json,
    load_settlement_report_records,
    performance_report_to_record,
)
from app.fqis.reporting.hybrid_shadow_report import (
    build_hybrid_shadow_batch_report_from_jsonl,
    hybrid_shadow_batch_report_to_record,
)


AuditSeverity = Literal["INFO", "WARN", "FAIL"]


@dataclass(slots=True, frozen=True)
class RunAuditThresholds:
    min_bet_count: int = 1
    min_clv_beat_rate: float = 0.50
    max_brier_score: float = 0.25
    max_abs_delta_model_market_mean: float = 0.25


@dataclass(slots=True, frozen=True)
class RunAuditFlag:
    severity: AuditSeverity
    code: str
    detail: str


@dataclass(slots=True, frozen=True)
class RunAuditReport:
    status: str
    run_id: str
    generated_at_utc: str
    hybrid_batch_path: str
    settlement_path: str
    closing_path: str
    health_status: str
    audit_flags: tuple[RunAuditFlag, ...]
    headline_metrics: dict[str, Any]
    hybrid_batch_report: dict[str, Any]
    settlement_report: dict[str, Any]
    performance_report: dict[str, Any]
    clv_report: dict[str, Any]

    @property
    def flag_count(self) -> int:
        return len(self.audit_flags)

    @property
    def fail_count(self) -> int:
        return sum(1 for flag in self.audit_flags if flag.severity == "FAIL")

    @property
    def warn_count(self) -> int:
        return sum(1 for flag in self.audit_flags if flag.severity == "WARN")

    @property
    def info_count(self) -> int:
        return sum(1 for flag in self.audit_flags if flag.severity == "INFO")


def build_run_audit_report(
    *,
    hybrid_batch_path: Path,
    settlement_path: Path,
    closing_path: Path,
    run_id: str | None = None,
    thresholds: RunAuditThresholds | None = None,
) -> RunAuditReport:
    cfg = thresholds or RunAuditThresholds()

    hybrid_report = build_hybrid_shadow_batch_report_from_jsonl(hybrid_batch_path)
    hybrid_record = hybrid_shadow_batch_report_to_record(hybrid_report)

    settlement_records = load_settlement_report_records(settlement_path)
    settlement_record = _settlement_record_for_audit(settlement_records)

    performance_report = build_performance_report_from_json(settlement_path)
    performance_record = performance_report_to_record(performance_report)

    clv_report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=closing_path,
    )
    clv_record = clv_report_to_record(clv_report)

    headline_metrics = _build_headline_metrics(
        hybrid_record=hybrid_record,
        settlement_record=settlement_record,
        performance_record=performance_record,
        clv_record=clv_record,
    )

    flags = _build_audit_flags(
        hybrid_record=hybrid_record,
        settlement_record=settlement_record,
        performance_record=performance_record,
        clv_record=clv_record,
        thresholds=cfg,
    )

    return RunAuditReport(
        status="ok",
        run_id=run_id or _default_run_id(),
        generated_at_utc=datetime.now(UTC).isoformat(),
        hybrid_batch_path=str(hybrid_batch_path),
        settlement_path=str(settlement_path),
        closing_path=str(closing_path),
        health_status=_health_status(flags),
        audit_flags=flags,
        headline_metrics=headline_metrics,
        hybrid_batch_report=hybrid_record,
        settlement_report=settlement_record,
        performance_report=performance_record,
        clv_report=clv_record,
    )


def run_audit_report_to_record(report: RunAuditReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_run_audit_report",
        "run_id": report.run_id,
        "generated_at_utc": report.generated_at_utc,
        "health_status": report.health_status,
        "flag_count": report.flag_count,
        "fail_count": report.fail_count,
        "warn_count": report.warn_count,
        "info_count": report.info_count,
        "inputs": {
            "hybrid_batch_path": report.hybrid_batch_path,
            "settlement_path": report.settlement_path,
            "closing_path": report.closing_path,
        },
        "headline_metrics": dict(report.headline_metrics),
        "audit_flags": [
            _audit_flag_to_record(flag)
            for flag in report.audit_flags
        ],
        "reports": {
            "hybrid_batch": dict(report.hybrid_batch_report),
            "settlement": dict(report.settlement_report),
            "performance": dict(report.performance_report),
            "clv": dict(report.clv_report),
        },
    }


def write_run_audit_report_json(report: RunAuditReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            run_audit_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _build_headline_metrics(
    *,
    hybrid_record: dict[str, Any],
    settlement_record: dict[str, Any],
    performance_record: dict[str, Any],
    clv_record: dict[str, Any],
) -> dict[str, Any]:
    return {
        "match_count": hybrid_record.get("match_count"),
        "accepted_match_count": hybrid_record.get("accepted_match_count"),
        "accepted_bet_count": settlement_record.get("accepted_bet_count"),
        "settled_bet_count": settlement_record.get("settled_bet_count"),
        "unsettled_bet_count": settlement_record.get("unsettled_bet_count"),
        "hybrid_probability_count": hybrid_record.get("hybrid_probability_count"),
        "hybrid_count": hybrid_record.get("hybrid_count"),
        "model_only_count": hybrid_record.get("model_only_count"),
        "p_hybrid_mean": _nested_numeric(
            hybrid_record,
            ("numeric_summaries", "p_hybrid", "mean"),
        ),
        "delta_model_market_mean": _nested_numeric(
            hybrid_record,
            ("numeric_summaries", "delta_model_market", "mean"),
        ),
        "roi": performance_record.get("roi"),
        "hit_rate": performance_record.get("hit_rate"),
        "brier_score": performance_record.get("brier_score"),
        "average_odds": performance_record.get("average_odds"),
        "average_p_real": performance_record.get("average_p_real"),
        "total_profit": settlement_record.get("total_profit"),
        "clv_beat_rate": clv_record.get("beat_rate"),
        "average_clv_percent": clv_record.get("average_clv_percent"),
        "average_clv_odds_delta": clv_record.get("average_clv_odds_delta"),
        "clv_missing_count": clv_record.get("missing_count"),
    }


def _build_audit_flags(
    *,
    hybrid_record: dict[str, Any],
    settlement_record: dict[str, Any],
    performance_record: dict[str, Any],
    clv_record: dict[str, Any],
    thresholds: RunAuditThresholds,
) -> tuple[RunAuditFlag, ...]:
    flags: list[RunAuditFlag] = []

    accepted_bet_count = int(settlement_record.get("accepted_bet_count") or 0)
    graded_bet_count = int(performance_record.get("graded_bet_count") or 0)
    model_only_count = int(hybrid_record.get("model_only_count") or 0)
    clv_missing_count = int(clv_record.get("missing_count") or 0)

    if accepted_bet_count < thresholds.min_bet_count:
        flags.append(
            RunAuditFlag(
                severity="FAIL",
                code="NO_ACCEPTED_BETS",
                detail=f"accepted_bet_count={accepted_bet_count} below minimum {thresholds.min_bet_count}",
            )
        )

    if graded_bet_count == 0:
        flags.append(
            RunAuditFlag(
                severity="FAIL",
                code="NO_GRADED_BETS",
                detail="No WON/LOST bets available for performance scoring.",
            )
        )

    roi = _optional_float(performance_record.get("roi"))
    if roi is not None and roi < 0.0:
        flags.append(
            RunAuditFlag(
                severity="WARN",
                code="NEGATIVE_ROI",
                detail=f"ROI is negative: {roi:.6f}",
            )
        )

    brier_score = _optional_float(performance_record.get("brier_score"))
    if brier_score is not None and brier_score > thresholds.max_brier_score:
        flags.append(
            RunAuditFlag(
                severity="WARN",
                code="HIGH_BRIER_SCORE",
                detail=f"Brier score {brier_score:.6f} exceeds threshold {thresholds.max_brier_score:.6f}",
            )
        )

    clv_beat_rate = _optional_float(clv_record.get("beat_rate"))
    if clv_beat_rate is not None and clv_beat_rate < thresholds.min_clv_beat_rate:
        flags.append(
            RunAuditFlag(
                severity="WARN",
                code="LOW_CLV_BEAT_RATE",
                detail=f"CLV beat rate {clv_beat_rate:.6f} below threshold {thresholds.min_clv_beat_rate:.6f}",
            )
        )

    if clv_missing_count > 0:
        flags.append(
            RunAuditFlag(
                severity="WARN",
                code="MISSING_CLOSING_ODDS",
                detail=f"{clv_missing_count} bets have no closing odds.",
            )
        )

    delta_model_market_mean = _nested_numeric(
        hybrid_record,
        ("numeric_summaries", "delta_model_market", "mean"),
    )
    if (
        delta_model_market_mean is not None
        and abs(delta_model_market_mean) > thresholds.max_abs_delta_model_market_mean
    ):
        flags.append(
            RunAuditFlag(
                severity="WARN",
                code="HIGH_MODEL_MARKET_DELTA_MEAN",
                detail=(
                    f"Mean model-market delta {delta_model_market_mean:.6f} exceeds "
                    f"threshold {thresholds.max_abs_delta_model_market_mean:.6f}"
                ),
            )
        )

    if model_only_count > 0:
        flags.append(
            RunAuditFlag(
                severity="INFO",
                code="MODEL_ONLY_PROBABILITIES_PRESENT",
                detail=f"{model_only_count} probabilities used model-only fallback.",
            )
        )

    return tuple(flags)


def _health_status(flags: tuple[RunAuditFlag, ...]) -> str:
    if any(flag.severity == "FAIL" for flag in flags):
        return "FAIL"

    if any(flag.severity == "WARN" for flag in flags):
        return "WARN"

    return "PASS"


def _settlement_record_for_audit(records: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    if len(records) == 1:
        return dict(records[0])

    return {
        "source": "fqis_settlement_report_collection",
        "report_count": len(records),
        "accepted_bet_count": sum(int(record.get("accepted_bet_count") or 0) for record in records),
        "settled_bet_count": sum(int(record.get("settled_bet_count") or 0) for record in records),
        "unsettled_bet_count": sum(int(record.get("unsettled_bet_count") or 0) for record in records),
        "won_count": sum(int(record.get("won_count") or 0) for record in records),
        "lost_count": sum(int(record.get("lost_count") or 0) for record in records),
        "push_count": sum(int(record.get("push_count") or 0) for record in records),
        "total_staked": sum(float(record.get("total_staked") or 0.0) for record in records),
        "total_profit": sum(float(record.get("total_profit") or 0.0) for record in records),
        "records": [dict(record) for record in records],
    }


def _audit_flag_to_record(flag: RunAuditFlag) -> dict[str, str]:
    return {
        "severity": flag.severity,
        "code": flag.code,
        "detail": flag.detail,
    }


def _nested_numeric(record: dict[str, Any], path: tuple[str, ...]) -> float | None:
    current: Any = record

    for key in path:
        if not isinstance(current, dict):
            return None

        current = current.get(key)

    return _optional_float(current)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    return float(value)


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("fqis_run_audit_%Y%m%d_%H%M%S")

    