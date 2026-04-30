from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
MONITOR_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
SHADOW_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
OPERATOR_CONSOLE_JSON = ORCH_DIR / "latest_operator_shadow_console.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
DISCORD_PAPER_PAYLOAD_JSON = ORCH_DIR / "latest_discord_paper_payload.json"
OUT_JSON = ORCH_DIR / "latest_tonight_shadow_digest.json"
OUT_MD = ORCH_DIR / "latest_tonight_shadow_digest.md"
FULL_CYCLE_MD = ORCH_DIR / "latest_full_cycle_report.md"
HISTORICAL_STATIC_REVIEW_FLAGS = {
    "CONSTANT_POST_QUARANTINE_PNL_REVIEW",
    "CONSTANT_FIXTURE_PNL_REVIEW",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = fnum(row.get(key))
        if value is not None:
            values.append(value)
    return values


def summary_value(summary: dict[str, Any], rows: list[dict[str, Any]], key: str) -> Any:
    if key in summary:
        return summary.get(key)

    values = numeric_values(rows, key.removeprefix("min_").removeprefix("max_"))
    if not values:
        return None
    if key.startswith("min_"):
        return min(values)
    if key.startswith("max_"):
        return max(values)
    return None


def monitor_all_ledger_preserved(summary: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    if "all_ledger_preserved" in summary:
        return summary.get("all_ledger_preserved") is True
    return bool(rows) and all(row.get("ledger_preserved") is True for row in rows)


def build_recommendations(verdict: str, stopped_reason: str, freshness_flags: list[Any]) -> list[str]:
    if verdict == "SHADOW_SESSION_CLEAN":
        return ["Continue paper-only shadow monitoring. Do not enable real staking."]
    if verdict == "SHADOW_SESSION_CLEAN_WITH_STALE_REVIEW":
        flags = ", ".join(str(flag) for flag in freshness_flags) if freshness_flags else "STALE_REVIEW"
        return [
            "Continue paper-only shadow monitoring. Do not enable real staking.",
            f"Review live freshness flags before interpreting research performance: {flags}.",
        ]
    if verdict == "SHADOW_SESSION_CLEAN_WITH_PAPER_ALERTS":
        return [
            "Continue paper-only shadow monitoring. Treat paper alerts as observation only.",
            "Do not place real bets, size stakes, or enable live staking.",
        ]
    reason = stopped_reason or "NONE"
    return [
        f"Stopped reason: {reason}. Inspect {FULL_CYCLE_MD.relative_to(ROOT)}.",
    ]


def build_digest() -> dict[str, Any]:
    monitor = read_json(MONITOR_JSON)
    full_cycle = read_json(FULL_CYCLE_JSON)
    shadow_report = read_json(SHADOW_JSON)
    live_freshness_report = read_json(LIVE_FRESHNESS_JSON)
    operator_console = read_json(OPERATOR_CONSOLE_JSON)
    paper_dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    paper_ranker = read_json(PAPER_ALERT_RANKER_JSON)
    discord_payload = read_json(DISCORD_PAPER_PAYLOAD_JSON)

    rows = monitor.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    rows = [row for row in rows if isinstance(row, dict)]

    summary = monitor.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}

    reports = full_cycle.get("reports") or {}
    go_no_go_report = reports.get("go_no_go") or {}
    shadow_from_full_cycle = reports.get("shadow_readiness") or {}
    live_freshness_from_full_cycle = reports.get("live_freshness") or {}
    operator_from_full_cycle = reports.get("operator_shadow_console") or {}
    daily_audit = reports.get("daily_audit") or {}
    daily_verdict = daily_audit.get("verdict") or {}
    invariants = full_cycle.get("invariants") or {}
    final_row = rows[-1] if rows else {}

    monitor_status = monitor.get("status") or "UNKNOWN"
    cycles_completed = int(monitor.get("cycles_completed") or len(rows))
    cycles_requested = int(monitor.get("cycles_requested") or 0)
    stopped_reason = str(monitor.get("stopped_reason") or "")

    ledger_preserved_final = invariants.get("research_candidates_ledger_preserved")
    if ledger_preserved_final is None:
        ledger_preserved_final = final_row.get("ledger_preserved")

    all_ledger_preserved = monitor_all_ledger_preserved(summary, rows)
    any_real_bets_enabled = (
        summary.get("any_real_bets_enabled") is True
        or any(row.get("can_execute_real_bets") is True for row in rows)
        or shadow_report.get("can_execute_real_bets") is True
        or shadow_from_full_cycle.get("can_execute_real_bets") is True
    )
    any_live_staking_enabled = (
        summary.get("any_live_staking_enabled") is True
        or any(
            row.get("live_staking_allowed") is True or row.get("can_enable_live_staking") is True
            for row in rows
        )
        or go_no_go_report.get("live_staking_allowed") is True
        or invariants.get("live_staking_enabled") is True
        or shadow_report.get("can_enable_live_staking") is True
        or shadow_from_full_cycle.get("can_enable_live_staking") is True
    )
    any_promotion_allowed = (
        summary.get("any_promotion_allowed") is True
        or any(row.get("promotion_allowed") is True for row in rows)
        or go_no_go_report.get("promotion_allowed") is True
        or daily_verdict.get("promotion_allowed") is True
    )
    final_operator_state = (
        operator_console.get("operator_state")
        or operator_from_full_cycle.get("operator_state")
        or final_row.get("operator_state")
    )
    total_new_paper_alerts = summary.get("total_new_paper_alerts")
    if total_new_paper_alerts is None:
        total_new_paper_alerts = sum(int(row.get("new_paper_alerts") or 0) for row in rows)
        if not rows:
            total_new_paper_alerts = paper_dedupe.get("new_alerts", 0)
    total_repeated_paper_alerts = summary.get("total_repeated_paper_alerts")
    if total_repeated_paper_alerts is None:
        total_repeated_paper_alerts = sum(int(row.get("repeated_paper_alerts") or 0) for row in rows)
        if not rows:
            total_repeated_paper_alerts = paper_dedupe.get("repeated_alerts", 0)
    any_sendable_discord_payload = (
        summary.get("any_sendable_discord_payload") is True
        or any(row.get("sendable_discord_payload") is True for row in rows)
        or discord_payload.get("sendable") is True
    )
    final_paper_signals_total = (
        final_row.get("paper_signals_total")
        if final_row.get("paper_signals_total") is not None
        else operator_console.get("total_paper_signals")
    )
    final_ranked_alert_count = (
        final_row.get("ranked_alert_count")
        if final_row.get("ranked_alert_count") is not None
        else paper_ranker.get("ranked_alert_count")
    )

    unsafe_flags = (
        ledger_preserved_final is not True
        or all_ledger_preserved is not True
        or any_real_bets_enabled
        or any_live_staking_enabled
        or any_promotion_allowed
        or final_operator_state == "PAPER_BLOCKED"
    )

    final_live_freshness_status = (
        live_freshness_report.get("status")
        or live_freshness_from_full_cycle.get("status")
        or final_row.get("live_freshness_status")
    )
    freshness_flags_final = (
        live_freshness_report.get("freshness_flags")
        or live_freshness_from_full_cycle.get("freshness_flags")
        or final_row.get("freshness_flags")
        or []
    )
    historical_static_review_final = (
        live_freshness_report.get("historical_metric_static_review")
        or live_freshness_from_full_cycle.get("historical_metric_static_review")
        or []
    )
    review_freshness_flags = [
        flag
        for flag in freshness_flags_final
        if flag != "OK_FRESH_LIVE_CYCLE" and flag not in HISTORICAL_STATIC_REVIEW_FLAGS
    ]
    total_new_snapshots_appended = summary.get("total_new_snapshots_appended")
    if total_new_snapshots_appended is None:
        total_new_snapshots_appended = sum(int(row.get("new_snapshots_appended") or 0) for row in rows)

    clean_session = monitor_status in {"READY", "MANUALLY_INTERRUPTED"} and not unsafe_flags and cycles_completed > 0

    if monitor_status == "STOPPED":
        verdict = "SHADOW_SESSION_STOPPED"
    elif unsafe_flags:
        verdict = "SHADOW_SESSION_INVALID"
    elif clean_session and int(total_new_paper_alerts or 0) > 0:
        verdict = "SHADOW_SESSION_CLEAN_WITH_PAPER_ALERTS"
    elif clean_session and (final_live_freshness_status == "STALE_REVIEW" or bool(review_freshness_flags)):
        verdict = "SHADOW_SESSION_CLEAN_WITH_STALE_REVIEW"
    elif clean_session:
        verdict = "SHADOW_SESSION_CLEAN"
    else:
        verdict = "SHADOW_SESSION_INVALID"

    payload = {
        "status": "STOPPED" if monitor_status == "STOPPED" else "READY",
        "generated_at_utc": utc_now(),
        "monitor_status": monitor_status,
        "cycles_completed": cycles_completed,
        "cycles_requested": cycles_requested,
        "stopped_reason": stopped_reason,
        "final_full_cycle_status": full_cycle.get("status") or final_row.get("full_cycle_status"),
        "final_go_no_go_state": go_no_go_report.get("go_no_go_state") or final_row.get("go_no_go_state"),
        "final_shadow_state": (
            shadow_report.get("shadow_state")
            or shadow_from_full_cycle.get("shadow_state")
            or final_row.get("shadow_state")
        ),
        "final_operator_state": final_operator_state,
        "ledger_preserved_final": ledger_preserved_final,
        "all_ledger_preserved": all_ledger_preserved,
        "any_real_bets_enabled": any_real_bets_enabled,
        "any_live_staking_enabled": any_live_staking_enabled,
        "any_promotion_allowed": any_promotion_allowed,
        "total_new_paper_alerts": total_new_paper_alerts,
        "total_repeated_paper_alerts": total_repeated_paper_alerts,
        "any_sendable_discord_payload": any_sendable_discord_payload,
        "final_paper_signals_total": final_paper_signals_total,
        "final_ranked_alert_count": final_ranked_alert_count,
        "min_post_quarantine_pnl": summary_value(summary, rows, "min_post_quarantine_pnl"),
        "max_post_quarantine_pnl": summary_value(summary, rows, "max_post_quarantine_pnl"),
        "min_post_quarantine_roi": summary_value(summary, rows, "min_post_quarantine_roi"),
        "max_post_quarantine_roi": summary_value(summary, rows, "max_post_quarantine_roi"),
        "final_live_freshness_status": final_live_freshness_status,
        "total_new_snapshots_appended": total_new_snapshots_appended,
        "freshness_flags_final": freshness_flags_final,
        "historical_static_review_final": historical_static_review_final,
        "verdict": verdict,
        "recommendations": build_recommendations(verdict, stopped_reason, freshness_flags_final),
    }
    return payload


def write_markdown(payload: dict[str, Any]) -> None:
    fields = [
        "status",
        "monitor_status",
        "cycles_completed",
        "cycles_requested",
        "stopped_reason",
        "final_full_cycle_status",
        "final_go_no_go_state",
        "final_shadow_state",
        "final_operator_state",
        "ledger_preserved_final",
        "all_ledger_preserved",
        "any_real_bets_enabled",
        "any_live_staking_enabled",
        "any_promotion_allowed",
        "total_new_paper_alerts",
        "total_repeated_paper_alerts",
        "any_sendable_discord_payload",
        "final_paper_signals_total",
        "final_ranked_alert_count",
        "min_post_quarantine_pnl",
        "max_post_quarantine_pnl",
        "min_post_quarantine_roi",
        "max_post_quarantine_roi",
        "final_live_freshness_status",
        "total_new_snapshots_appended",
        "freshness_flags_final",
        "historical_static_review_final",
        "verdict",
    ]

    lines = [
        "# FQIS Tonight Shadow Digest",
        "",
        *[f"- {field}: **{payload.get(field)}**" for field in fields],
        "",
        "## Recommendations",
        "",
    ]
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(payload: dict[str, Any]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)


def main() -> int:
    payload = build_digest()
    write_outputs(payload)
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
