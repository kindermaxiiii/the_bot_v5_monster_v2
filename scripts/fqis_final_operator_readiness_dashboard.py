from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

OUT_JSON = ORCH_DIR / "latest_final_operator_readiness_dashboard.json"
OUT_MD = ORCH_DIR / "latest_final_operator_readiness_dashboard.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
    "live_execution_enabled": False,
    "discord_send_performed": False,
    "ledger_mutation_performed": False,
}

LIVE_ENABLE_FLAGS = {
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
    "live_execution_enabled",
}

MIN_SIGNAL_SAMPLE_SIZE = 30
MIN_SETTLED_SIGNAL_SAMPLE_SIZE = 30

DEFAULT_INPUTS = {
    "full_cycle": [ORCH_DIR / "latest_full_cycle_report.json"],
    "operator_shadow_console": [ORCH_DIR / "latest_operator_shadow_console.json"],
    "signal_settlement": [
        RESEARCH_DIR / "latest_signal_settlement_report.json",
        ORCH_DIR / "latest_signal_settlement_report.json",
    ],
    "calibration": [
        RESEARCH_DIR / "latest_calibration_report.json",
        ORCH_DIR / "latest_calibration_report.json",
    ],
    "proxy_clv": [ORCH_DIR / "latest_clv_tracker_report.json"],
    "promotion_policy": [ORCH_DIR / "latest_promotion_policy_report.json"],
    "discord_preview": [ORCH_DIR / "latest_discord_alert_renderer.json"],
    "live_freshness": [ORCH_DIR / "latest_live_freshness_report.json"],
    "go_no_go": [ORCH_DIR / "latest_go_no_go_report.json"],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            return {"error": "JSON_ROOT_NOT_OBJECT", "path": str(path)}
        return payload
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def resolve_input(paths: list[Path]) -> tuple[Path, dict[str, Any]]:
    for path in paths:
        if path.exists():
            return path, read_json(path)
    path = paths[0]
    return path, {"missing": True, "path": str(path)}


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def add_unique(items: list[str], code: str) -> None:
    if code not in items:
        items.append(code)


def unsafe_true_flags(payload: Any, prefix: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in LIVE_ENABLE_FLAGS and value is True:
                hits.append(path)
            hits.extend(unsafe_true_flags(value, path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            hits.extend(unsafe_true_flags(value, f"{prefix}[{index}]"))
    return hits


def input_status(name: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": payload.get("status", "MISSING" if payload.get("missing") else "UNKNOWN"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "missing": payload.get("missing") is True,
        "error": payload.get("error"),
    }


def build_inputs(input_paths: dict[str, list[Path]]) -> tuple[dict[str, dict[str, Any]], dict[str, Path]]:
    reports: dict[str, dict[str, Any]] = {}
    resolved: dict[str, Path] = {}
    for name, paths in input_paths.items():
        path, payload = resolve_input(paths)
        resolved[name] = path
        reports[name] = payload
    return reports, resolved


def paper_trading_summary(reports: dict[str, dict[str, Any]]) -> dict[str, Any]:
    full_cycle = reports["full_cycle"]
    nested = full_cycle.get("reports") or {}
    paper_export = nested.get("paper_signal_export") or {}
    paper_ranker = nested.get("paper_alert_ranker") or {}
    operator = reports["operator_shadow_console"]
    go_no_go = reports["go_no_go"]
    return {
        "full_cycle_status": full_cycle.get("status"),
        "operator_state": operator.get("operator_state"),
        "operator_next_action": operator.get("next_action"),
        "paper_signals_total": operator.get(
            "total_paper_signals",
            paper_export.get("paper_signals_total", paper_export.get("total_decisions", 0)),
        ),
        "top_ranked_alert_count": operator.get(
            "top_ranked_alert_count",
            paper_ranker.get("top_ranked_alert_count", paper_ranker.get("ranked_alert_count", 0)),
        ),
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "simulation_only": go_no_go.get("simulation_only", True),
    }


def signal_settlement_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "total_signals": as_int(report.get("total_signals")),
        "settled_signals": as_int(report.get("settled_signals")),
        "unsettled_signals": as_int(report.get("unsettled_signals")),
        "win_count": as_int(report.get("win_count")),
        "loss_count": as_int(report.get("loss_count")),
        "push_count": as_int(report.get("push_count")),
        "unknown_count": as_int(report.get("unknown_count")),
        "paper_pnl_total": as_float(report.get("paper_pnl_total")),
        "paper_roi": as_float(report.get("paper_roi")),
        "warning_flags": report.get("warning_flags") or [],
    }


def calibration_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "total_rows": as_int(report.get("total_rows")),
        "eligible_settled_rows": as_int(report.get("eligible_settled_rows")),
        "brier_score": report.get("brier_score"),
        "log_loss": report.get("log_loss"),
        "warning_flags": report.get("warning_flags") or [],
    }


def proxy_clv_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "total_records": as_int(report.get("total_records")),
        "eligible_records": as_int(report.get("eligible_records")),
        "favorable_move_count": as_int(report.get("favorable_move_count")),
        "unfavorable_move_count": as_int(report.get("unfavorable_move_count")),
        "flat_move_count": as_int(report.get("flat_move_count")),
        "favorable_move_rate": as_float(report.get("favorable_move_rate")),
        "warning_flags": report.get("warning_flags") or [],
    }


def promotion_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "final_verdict": report.get("final_verdict"),
        "promotion_allowed": False,
        "promotion_allowed_count": as_int(report.get("promotion_allowed_count")),
        "paper_elite_candidate_count": as_int(report.get("paper_elite_candidate_count")),
        "warning_flags": report.get("warning_flags") or [],
    }


def discord_preview_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": report.get("status"),
        "elite_alerts_count": as_int(report.get("elite_alerts_count")),
        "model_logs_count": as_int(report.get("model_logs_count")),
        "no_send_count": as_int(report.get("no_send_count")),
        "discord_send_performed": False,
        "purpose": (report.get("read") or {}).get("purpose", "PRESENTATION_ONLY"),
    }


def collect_blockers(reports: dict[str, dict[str, Any]]) -> tuple[list[str], list[str]]:
    hard: list[str] = []
    soft: list[str] = []

    for name, payload in reports.items():
        if payload.get("missing"):
            add_unique(hard, f"MISSING_INPUT_REPORT:{name}")
        if payload.get("error"):
            add_unique(hard, f"INPUT_REPORT_READ_ERROR:{name}")
        status = payload.get("status")
        if status in {"BLOCKED", "PARTIAL_FAILURE"}:
            add_unique(hard, f"INPUT_REPORT_NOT_READY:{name}:{status}")
        elif status in {"REVIEW", "EMPTY", "MISSING", "UNKNOWN", None}:
            add_unique(soft, f"INPUT_REPORT_REVIEW_REQUIRED:{name}:{status or 'UNKNOWN'}")

        unsafe = unsafe_true_flags(payload)
        if unsafe:
            add_unique(hard, f"UNSAFE_TRUE_FLAG_IN_INPUT:{name}:{','.join(sorted(set(unsafe))[:8])}")

    promotion = reports["promotion_policy"]
    if promotion.get("final_verdict") == "NO_PROMOTION_KEEP_RESEARCH":
        add_unique(hard, "PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH")
    if promotion.get("promotion_allowed") is True:
        add_unique(hard, "PROMOTION_POLICY_UNSAFE_PROMOTION_ALLOWED_TRUE")

    go_no_go = reports["go_no_go"]
    if go_no_go.get("promotion_allowed") is True or go_no_go.get("live_staking_allowed") is True:
        add_unique(hard, "GO_NO_GO_UNSAFE_LIVE_OR_PROMOTION_FLAG_TRUE")
    if go_no_go.get("go_no_go_state") != "NO_GO_DRY_RUN_ONLY":
        add_unique(soft, "GO_NO_GO_STATE_REVIEW_REQUIRED")

    settlement = reports["signal_settlement"]
    total_signals = as_int(settlement.get("total_signals"))
    settled_signals = as_int(settlement.get("settled_signals"))
    if total_signals == 0:
        add_unique(hard, "NO_SIGNAL_SETTLEMENT_SAMPLE_AVAILABLE")
    elif total_signals < MIN_SIGNAL_SAMPLE_SIZE:
        add_unique(soft, f"INSUFFICIENT_SIGNAL_SAMPLE_SIZE:{total_signals}<{MIN_SIGNAL_SAMPLE_SIZE}")
    if settled_signals == 0:
        add_unique(hard, "NO_SETTLED_SIGNAL_EVIDENCE_AVAILABLE")
    elif settled_signals < MIN_SETTLED_SIGNAL_SAMPLE_SIZE:
        add_unique(soft, f"INSUFFICIENT_SETTLED_SIGNAL_SAMPLE_SIZE:{settled_signals}<{MIN_SETTLED_SIGNAL_SAMPLE_SIZE}")

    for flag in settlement.get("warning_flags") or []:
        if "INSUFFICIENT" in str(flag).upper() or "NO_SETTLED" in str(flag).upper():
            add_unique(soft, f"SIGNAL_SETTLEMENT_WARNING:{safe_text(flag)}")

    calibration = reports["calibration"]
    if calibration.get("eligible_settled_rows") is not None and as_int(calibration.get("eligible_settled_rows")) == 0:
        add_unique(soft, "CALIBRATION_HAS_NO_ELIGIBLE_SETTLED_ROWS")
    for flag in calibration.get("warning_flags") or []:
        if "INSUFFICIENT" in str(flag).upper():
            add_unique(soft, f"CALIBRATION_WARNING:{safe_text(flag)}")

    clv = reports["proxy_clv"]
    if clv.get("eligible_records") is not None and as_int(clv.get("eligible_records")) == 0:
        add_unique(soft, "PROXY_CLV_HAS_NO_ELIGIBLE_RECORDS")
    for flag in clv.get("warning_flags") or []:
        if "INSUFFICIENT" in str(flag).upper():
            add_unique(soft, f"PROXY_CLV_WARNING:{safe_text(flag)}")

    return hard, soft


def readiness_score(hard_blockers: list[str], soft_blockers: list[str], reports: dict[str, dict[str, Any]]) -> int:
    score = 100
    score -= min(70, len(hard_blockers) * 18)
    score -= min(30, len(soft_blockers) * 5)

    if reports["promotion_policy"].get("final_verdict") == "PAPER_ELITE_CANDIDATE_REVIEW":
        score += 5
    if reports["signal_settlement"].get("status") == "READY":
        score += 5
    if reports["calibration"].get("status") == "READY":
        score += 5
    if reports["proxy_clv"].get("status") == "READY":
        score += 5

    return max(0, min(100, int(score)))


def readiness_level(
    *,
    score: int,
    hard_blockers: list[str],
    soft_blockers: list[str],
    promotion: dict[str, Any],
) -> str:
    if hard_blockers:
        return "BLOCKED"
    if score >= 95 and promotion.get("final_verdict") == "PAPER_ELITE_CANDIDATE_REVIEW":
        return "LIVE_READY_FORBIDDEN"
    if score >= 80 and promotion.get("paper_elite_candidate_count", 0):
        return "PROMOTION_REVIEW_CANDIDATE"
    if score >= 65 and len(soft_blockers) <= 3:
        return "PAPER_VALIDATION_READY"
    return "RESEARCH_READY"


def next_actions(level: str, hard_blockers: list[str], soft_blockers: list[str]) -> list[str]:
    actions: list[str] = []
    if hard_blockers:
        actions.append("Resolve hard blockers before any operator promotion review.")
    if any("PROMOTION_POLICY_NO_PROMOTION_KEEP_RESEARCH" in item for item in hard_blockers):
        actions.append("Keep research mode until promotion policy produces PAPER_ELITE_CANDIDATE_REVIEW.")
    if any("SAMPLE" in item or "EVIDENCE" in item for item in [*hard_blockers, *soft_blockers]):
        actions.append("Accumulate more paper-settled signal evidence before escalating readiness.")
    if level == "LIVE_READY_FORBIDDEN":
        actions.append("Treat maturity as review-only; real execution remains explicitly forbidden.")
    actions.append("Continue PAPER ONLY operations; do not enable live staking, ledger mutation, or Discord sending.")
    return list(dict.fromkeys(actions))


def build_dashboard(
    input_paths: dict[str, list[Path]] | None = None,
) -> dict[str, Any]:
    reports, resolved = build_inputs(input_paths or DEFAULT_INPUTS)
    hard_blockers, soft_blockers = collect_blockers(reports)
    score = readiness_score(hard_blockers, soft_blockers, reports)
    promotion = promotion_summary(reports["promotion_policy"])
    level = readiness_level(
        score=score,
        hard_blockers=hard_blockers,
        soft_blockers=soft_blockers,
        promotion=promotion,
    )
    status = "BLOCKED" if hard_blockers else ("REVIEW" if soft_blockers else "READY")

    return {
        "mode": "FQIS_FINAL_OPERATOR_READINESS_DASHBOARD",
        "status": status,
        "generated_at_utc": utc_now(),
        "system_state": {
            "execution_mode": "PAPER_ONLY",
            "operator_readiness_layer": "AGGREGATE_READ_ONLY",
            "live_readiness_label": "LIVE_READY_FORBIDDEN",
            **SAFETY_BLOCK,
        },
        "readiness_score_0_100": score,
        "readiness_level": level,
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "next_required_actions": next_actions(level, hard_blockers, soft_blockers),
        "safety": dict(SAFETY_BLOCK),
        "input_report_statuses": {
            name: input_status(name, resolved[name], reports[name])
            for name in DEFAULT_INPUTS
        },
        "paper_trading_summary": paper_trading_summary(reports),
        "signal_settlement_summary": signal_settlement_summary(reports["signal_settlement"]),
        "calibration_summary": calibration_summary(reports["calibration"]),
        "proxy_clv_summary": proxy_clv_summary(reports["proxy_clv"]),
        "promotion_summary": promotion,
        "discord_preview_summary": discord_preview_summary(reports["discord_preview"]),
        **SAFETY_BLOCK,
    }


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Final Operator Readiness Dashboard",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION | NO LEDGER MUTATION | NO DISCORD SEND",
        "",
        "## Readiness",
        "",
        f"- Status: **{report.get('status')}**",
        f"- Generated at UTC: `{report.get('generated_at_utc')}`",
        f"- Readiness score: **{report.get('readiness_score_0_100')} / 100**",
        f"- Readiness level: **{report.get('readiness_level')}**",
        f"- Execution mode: **{(report.get('system_state') or {}).get('execution_mode')}**",
        f"- Live execution enabled: **{report.get('live_execution_enabled', False)}**",
        f"- Promotion allowed: **{report.get('promotion_allowed', False)}**",
        "",
        "## Hard Blockers",
        "",
    ]
    hard = report.get("hard_blockers") or []
    lines.extend(f"- {safe_text(item)}" for item in hard) if hard else lines.append("- NONE")

    lines += ["", "## Soft Blockers", ""]
    soft = report.get("soft_blockers") or []
    lines.extend(f"- {safe_text(item)}" for item in soft) if soft else lines.append("- NONE")

    lines += ["", "## Next Required Actions", ""]
    for action in report.get("next_required_actions") or []:
        lines.append(f"- {safe_text(action)}")

    signal = report.get("signal_settlement_summary") or {}
    calibration = report.get("calibration_summary") or {}
    clv = report.get("proxy_clv_summary") or {}
    promotion = report.get("promotion_summary") or {}
    discord = report.get("discord_preview_summary") or {}

    lines += [
        "",
        "## Evidence Summary",
        "",
        f"- Signal settlement: **{signal.get('status')}**, total **{signal.get('total_signals')}**, settled **{signal.get('settled_signals')}**, ROI **{signal.get('paper_roi')}**",
        f"- Calibration: **{calibration.get('status')}**, eligible settled rows **{calibration.get('eligible_settled_rows')}**, Brier **{calibration.get('brier_score')}**",
        f"- Proxy CLV: **{clv.get('status')}**, eligible records **{clv.get('eligible_records')}**, favorable rate **{clv.get('favorable_move_rate')}**",
        f"- Promotion policy: **{promotion.get('status')}**, verdict **{promotion.get('final_verdict')}**, promotion allowed **False**",
        f"- Discord preview: **{discord.get('status')}**, elite alerts **{discord.get('elite_alerts_count')}**, send performed **False**",
        "",
        "## Input Reports",
        "",
        "| Report | Status | Missing | Path |",
        "|---|---|---:|---|",
    ]
    for name, item in (report.get("input_report_statuses") or {}).items():
        lines.append(
            f"| {safe_text(name)} | {safe_text(item.get('status'))} | {item.get('missing')} | {safe_text(item.get('path'))} |"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parser_with_inputs() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build final FQIS read-only operator readiness dashboard.")
    for name, paths in DEFAULT_INPUTS.items():
        parser.add_argument(f"--{name.replace('_', '-')}-path", default=str(paths[0]))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    return parser


def main() -> int:
    parser = parser_with_inputs()
    args = parser.parse_args()
    input_paths = {
        name: [Path(getattr(args, f"{name}_path"))]
        for name in DEFAULT_INPUTS
    }
    report = build_dashboard(input_paths)
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))

    print(
        json.dumps(
            {
                "status": report["status"],
                "readiness_level": report["readiness_level"],
                "readiness_score_0_100": report["readiness_score_0_100"],
                "hard_blockers": len(report["hard_blockers"]),
                "soft_blockers": len(report["soft_blockers"]),
                "output_json": str(Path(args.output_json)),
                "output_md": str(Path(args.output_md)),
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "promotion_allowed": False,
                "live_execution_enabled": False,
            },
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
