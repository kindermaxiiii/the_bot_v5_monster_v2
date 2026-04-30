from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_READINESS_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
LIVE_OPPORTUNITY_SCANNER_JSON = ORCH_DIR / "latest_live_opportunity_scanner.json"
LEVEL3_STATS_COVERAGE_DIAGNOSTIC_JSON = ORCH_DIR / "latest_level3_stats_coverage_diagnostic.json"
PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
CLV_TRACKER_JSON = ORCH_DIR / "latest_clv_tracker_report.json"
CALIBRATION_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_calibration_report.json"
PROMOTION_POLICY_JSON = ORCH_DIR / "latest_promotion_policy_report.json"
OPERATOR_PAPER_DECISION_SHEET_JSON = ORCH_DIR / "latest_operator_paper_decision_sheet.json"
DISCORD_PAPER_PAYLOAD_JSON = ORCH_DIR / "latest_discord_paper_payload.json"
TONIGHT_MONITOR_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
TONIGHT_DIGEST_JSON = ORCH_DIR / "latest_tonight_shadow_digest.json"
OUT_JSON = ORCH_DIR / "latest_operator_shadow_console.json"
OUT_MD = ORCH_DIR / "latest_operator_shadow_console.md"

HISTORICAL_STATIC_REVIEW_FLAGS = {
    "CONSTANT_POST_QUARANTINE_PNL_REVIEW",
    "CONSTANT_FIXTURE_PNL_REVIEW",
}

LIVE_OPPORTUNITY_SCANNER_FIELDS = (
    "status",
    "generated_at_utc",
    "operator_read",
    "live_fixtures_seen",
    "groups_total",
    "groups_priced",
    "decisions_total",
    "candidates_this_cycle",
    "new_snapshots_appended",
    "level3_state_ready_count",
    "level3_trade_ready_count",
    "level3_events_available_count",
    "level3_stats_available_count",
    "score_only_decisions",
    "rejected_by_non_positive_edge_ev",
    "rejected_by_timing_policy",
    "rejected_by_data_tier",
    "rejected_by_final_status",
    "rejected_by_negative_value_veto",
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
    "read",
)
LEVEL3_STATS_COVERAGE_DIAGNOSTIC_FIELDS = (
    "fixtures_seen",
    "events_available",
    "raw_stats_available",
    "parsed_stats_available",
    "events_only_no_stats",
    "stats_parser_empty",
    "stats_endpoint_missing",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    try:
        if not value:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def truthy_flag(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def find_true_flags(payload: Any, names: set[str], prefix: str = "") -> list[str]:
    flags: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if str(key) in names and truthy_flag(value):
                flags.append(path)
            flags.extend(find_true_flags(value, names, path))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            flags.extend(find_true_flags(item, names, f"{prefix}[{index}]"))
    return flags


def safe_bool(*values: Any) -> bool:
    return any(truthy_flag(value) for value in values)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return default


def monitor_section(monitor: dict[str, Any], full_cycle: dict[str, Any], operator_generated_at_utc: str) -> dict[str, Any]:
    summary = monitor.get("summary") or {}
    cycles_completed = monitor.get("cycles_completed")
    cycles_requested = monitor.get("cycles_requested")
    has_cycles = bool(cycles_completed)
    monitor_generated_at = monitor.get("generated_at_utc")
    full_cycle_generated_at = full_cycle.get("generated_at_utc")
    monitor_dt = parse_timestamp(monitor_generated_at)
    full_cycle_dt = parse_timestamp(full_cycle_generated_at)
    monitor_complete = (
        has_cycles
        and (
            monitor.get("status") in {"READY", "STOPPED", "MANUALLY_INTERRUPTED"}
            and (not cycles_requested or int(cycles_completed or 0) >= int(cycles_requested or 0) or monitor.get("status") == "STOPPED")
        )
    )
    stale_for_current_full_cycle = bool(full_cycle_dt and monitor_dt and monitor_dt < full_cycle_dt)
    if not monitor or monitor.get("missing") or monitor.get("error"):
        context = "NO_MONITOR_CONTEXT"
    elif not monitor_complete or stale_for_current_full_cycle:
        context = "PARTIAL_MONITOR_CONTEXT"
    else:
        context = "FINAL_MONITOR_CONTEXT"
    return {
        "monitor_context": context,
        "monitor_artifact_generated_at_utc": monitor_generated_at,
        "operator_console_generated_at_utc": operator_generated_at_utc,
        "full_cycle_generated_at_utc": full_cycle_generated_at,
        "cycles_completed": cycles_completed,
        "cycles_requested": cycles_requested,
        "stopped_reason": monitor.get("stopped_reason"),
        "all_ledger_preserved": summary.get("all_ledger_preserved") if has_cycles else None,
        "any_real_bets_enabled": summary.get("any_real_bets_enabled") if has_cycles else None,
        "any_live_staking_enabled": summary.get("any_live_staking_enabled") if has_cycles else None,
        "any_promotion_allowed": summary.get("any_promotion_allowed") if has_cycles else None,
    }


def freshness_review_state(freshness: dict[str, Any]) -> dict[str, Any]:
    raw_flags = [str(flag) for flag in freshness.get("freshness_flags") or []]
    historical_flags = [str(flag) for flag in freshness.get("historical_metric_static_review") or []]
    legacy_historical = [flag for flag in raw_flags if flag in HISTORICAL_STATIC_REVIEW_FLAGS]
    live_review_flags = [
        flag
        for flag in raw_flags
        if flag != "OK_FRESH_LIVE_CYCLE" and flag not in HISTORICAL_STATIC_REVIEW_FLAGS
    ]
    historical_static_review = sorted(set([*historical_flags, *legacy_historical]))
    only_historical_static_review = bool(historical_static_review) and not live_review_flags
    freshness_ok_for_paper_ready = (
        freshness.get("status") == "READY"
        or (
            freshness.get("status") == "STALE_REVIEW"
            and only_historical_static_review
        )
    ) and freshness.get("status") != "MISSING_INPUTS"
    return {
        "flags": raw_flags,
        "live_review_flags": live_review_flags,
        "historical_metric_static_review": historical_static_review,
        "only_historical_static_review": only_historical_static_review,
        "freshness_ok_for_paper_ready": freshness_ok_for_paper_ready,
    }


def live_opportunity_scanner_section(scanner: dict[str, Any]) -> dict[str, Any]:
    section = {field: scanner.get(field) for field in LIVE_OPPORTUNITY_SCANNER_FIELDS}
    section["top_rejection_reasons"] = list(scanner.get("top_rejection_reasons") or [])[:3]
    if scanner.get("missing"):
        section["missing"] = True
        section["path"] = scanner.get("path")
    if scanner.get("error"):
        section["error"] = scanner.get("error")
        section["path"] = scanner.get("path")
    return section


def top_reason_counts(reason_counts: Any, limit: int = 5) -> list[dict[str, Any]]:
    items: list[tuple[str, int]] = []
    if isinstance(reason_counts, dict):
        items = [(str(reason), safe_int(count)) for reason, count in reason_counts.items()]
    elif isinstance(reason_counts, list):
        for item in reason_counts:
            if isinstance(item, dict):
                items.append((str(item.get("reason") or item.get("name") or ""), safe_int(item.get("count"))))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                items.append((str(item[0]), safe_int(item[1])))
    items = [(reason, count) for reason, count in items if reason]
    items.sort(key=lambda item: (-item[1], item[0]))
    return [{"reason": reason, "count": count} for reason, count in items[:limit]]


def level3_stats_coverage_diagnostic_section(diagnostic: dict[str, Any]) -> dict[str, Any]:
    summary = diagnostic.get("summary") or {}
    status = diagnostic.get("status")
    if not status and diagnostic.get("missing"):
        status = "MISSING"
    if not status and diagnostic.get("error"):
        status = "ERROR"

    section = {"status": status}
    for field in LEVEL3_STATS_COVERAGE_DIAGNOSTIC_FIELDS:
        section[field] = safe_int(summary.get(field))
    section["reason_counts"] = top_reason_counts(summary.get("reason_counts"), limit=5)

    if diagnostic.get("missing"):
        section["missing"] = True
        section["path"] = diagnostic.get("path")
    if diagnostic.get("error"):
        section["error"] = diagnostic.get("error")
        section["path"] = diagnostic.get("path")
    return section


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    full_cycle = read_json(FULL_CYCLE_JSON)
    reports = full_cycle.get("reports") or {}
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_READINESS_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)
    live_opportunity_scanner = read_json(LIVE_OPPORTUNITY_SCANNER_JSON)
    level3_stats_coverage_diagnostic = read_json(LEVEL3_STATS_COVERAGE_DIAGNOSTIC_JSON)
    paper_export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    ranker = read_json(PAPER_ALERT_RANKER_JSON)
    clv_tracker = read_json(CLV_TRACKER_JSON)
    calibration = read_json(CALIBRATION_JSON)
    promotion_policy = read_json(PROMOTION_POLICY_JSON)
    decision_sheet = read_json(OPERATOR_PAPER_DECISION_SHEET_JSON)
    discord_payload = read_json(DISCORD_PAPER_PAYLOAD_JSON)
    monitor = read_json(TONIGHT_MONITOR_JSON) if TONIGHT_MONITOR_JSON.exists() else {}
    digest = read_json(TONIGHT_DIGEST_JSON) if TONIGHT_DIGEST_JSON.exists() else {}

    daily_verdict = (reports.get("daily_audit") or {}).get("verdict") or {}
    invariants = full_cycle.get("invariants") or {}

    safety = {
        "promotion_allowed": safe_bool(
            go_no_go.get("promotion_allowed"),
            daily_verdict.get("promotion_allowed"),
            live_opportunity_scanner.get("promotion_allowed"),
            (live_opportunity_scanner.get("safety") or {}).get("promotion_allowed"),
            promotion_policy.get("promotion_allowed"),
            (promotion_policy.get("safety") or {}).get("promotion_allowed"),
        ),
        "live_staking_allowed": safe_bool(
            go_no_go.get("live_staking_allowed"),
            invariants.get("live_staking_enabled"),
            (paper_export.get("safety") or {}).get("live_staking_allowed"),
            live_opportunity_scanner.get("live_staking_allowed"),
            (live_opportunity_scanner.get("safety") or {}).get("live_staking_allowed"),
        ),
        "can_execute_real_bets": safe_bool(
            shadow.get("can_execute_real_bets"),
            paper_export.get("can_execute_real_bets"),
            ranker.get("can_execute_real_bets"),
            clv_tracker.get("can_execute_real_bets"),
            calibration.get("can_execute_real_bets"),
            promotion_policy.get("can_execute_real_bets"),
            decision_sheet.get("can_execute_real_bets"),
            discord_payload.get("can_execute_real_bets"),
            live_opportunity_scanner.get("can_execute_real_bets"),
            (live_opportunity_scanner.get("safety") or {}).get("can_execute_real_bets"),
        ),
        "can_enable_live_staking": safe_bool(
            shadow.get("can_enable_live_staking"),
            paper_export.get("can_enable_live_staking"),
            ranker.get("can_enable_live_staking"),
            clv_tracker.get("can_enable_live_staking"),
            calibration.get("can_enable_live_staking"),
            promotion_policy.get("can_enable_live_staking"),
            decision_sheet.get("can_enable_live_staking"),
            discord_payload.get("can_enable_live_staking"),
            live_opportunity_scanner.get("can_enable_live_staking"),
            (live_opportunity_scanner.get("safety") or {}).get("can_enable_live_staking"),
        ),
        "can_mutate_ledger": safe_bool(
            shadow.get("can_mutate_ledger"),
            paper_export.get("can_mutate_ledger"),
            dedupe.get("can_mutate_ledger"),
            ranker.get("can_mutate_ledger"),
            clv_tracker.get("can_mutate_ledger"),
            calibration.get("can_mutate_ledger"),
            promotion_policy.get("can_mutate_ledger"),
            decision_sheet.get("can_mutate_ledger"),
            discord_payload.get("can_mutate_ledger"),
            live_opportunity_scanner.get("can_mutate_ledger"),
            (live_opportunity_scanner.get("safety") or {}).get("can_mutate_ledger"),
        ),
    }

    inputs = {
        "full_cycle": full_cycle,
        "go_no_go": go_no_go,
        "shadow_readiness": shadow,
        "live_freshness": freshness,
        "paper_signal_export": paper_export,
        "paper_alert_dedupe": dedupe,
        "paper_alert_ranker": ranker,
        "operator_paper_decision_sheet": decision_sheet,
        "discord_paper_payload": discord_payload,
    }
    safety_inputs = {
        **inputs,
        "live_opportunity_scanner": live_opportunity_scanner,
        "clv_tracker": clv_tracker,
        "calibration": calibration,
        "promotion_policy": promotion_policy,
    }
    unsafe_names = {
        "live_staking_allowed",
        "level3_live_staking_allowed",
        "can_execute_real_bets",
        "can_enable_live_staking",
        "can_mutate_ledger",
        "promotion_allowed",
    }
    unsafe_hits: list[str] = []
    for name, payload in safety_inputs.items():
        unsafe_hits.extend(f"{name}:{hit}" for hit in find_true_flags(payload, unsafe_names))

    reasons: list[str] = []
    missing_required_input = False
    for name, payload in inputs.items():
        if payload.get("missing") or payload.get("error"):
            missing_required_input = True
            reasons.append(f"MISSING_INPUT:{name}")

    if unsafe_hits or any(safety.values()):
        reasons.append("UNSAFE_TRUE_FLAGS")
    if go_no_go.get("go_no_go_state") == "LIVE_READY":
        reasons.append("GO_NO_GO_LIVE_READY")
    if freshness.get("status") == "MISSING_INPUTS":
        reasons.append("LIVE_FRESHNESS_MISSING_INPUTS")
    if full_cycle.get("status") != "READY":
        reasons.append("FULL_CYCLE_NOT_READY")
    if shadow.get("shadow_state") != "SHADOW_READY":
        reasons.append("SHADOW_NOT_READY")
    if paper_export.get("status") == "BLOCKED":
        reasons.append("PAPER_SIGNAL_EXPORT_BLOCKED")
    if dedupe.get("status") == "BLOCKED":
        reasons.append("PAPER_ALERT_DEDUPE_BLOCKED")
    if ranker.get("status") == "BLOCKED":
        reasons.append("PAPER_ALERT_RANKER_BLOCKED")
    if decision_sheet.get("status") == "BLOCKED":
        reasons.append("OPERATOR_PAPER_DECISION_SHEET_BLOCKED")
    if discord_payload.get("status") == "BLOCKED":
        reasons.append("DISCORD_PAYLOAD_UNSAFE")

    freshness_review = freshness_review_state(freshness)
    ranked_alert_count = int(ranker.get("ranked_alert_count") or 0)
    top_ranked_alert_count = int(ranker.get("top_ranked_alert_count") or 0)
    no_useful_alerts = ranked_alert_count <= 0

    if (
        any(safety.values())
        or unsafe_hits
        or missing_required_input
        or go_no_go.get("go_no_go_state") == "LIVE_READY"
        or freshness.get("status") == "MISSING_INPUTS"
        or full_cycle.get("status") != "READY"
        or shadow.get("shadow_state") != "SHADOW_READY"
        or paper_export.get("status") == "BLOCKED"
        or dedupe.get("status") == "BLOCKED"
        or ranker.get("status") == "BLOCKED"
        or decision_sheet.get("status") == "BLOCKED"
        or discord_payload.get("status") == "BLOCKED"
    ):
        operator_state = "PAPER_BLOCKED"
        status = "BLOCKED"
        next_action = "STOP_SESSION"
    elif freshness_review["live_review_flags"]:
        operator_state = "PAPER_REVIEW"
        status = "REVIEW"
        next_action = "INSPECT_FRESHNESS"
        reasons.append("LIVE_FRESHNESS_LIVE_DATA_REVIEW")
    elif no_useful_alerts:
        operator_state = "PAPER_REVIEW"
        status = "REVIEW"
        next_action = "INSPECT_RANKER"
        reasons.append("NO_USEFUL_RANKED_PAPER_ALERTS")
    elif (
        full_cycle.get("status") == "READY"
        and shadow.get("shadow_state") == "SHADOW_READY"
        and paper_export.get("status") == "READY"
        and dedupe.get("status") == "READY"
        and ranker.get("status") == "READY"
        and decision_sheet.get("status") == "READY"
        and freshness_review["freshness_ok_for_paper_ready"]
    ):
        operator_state = "PAPER_READY"
        status = "READY"
        next_action = "CONTINUE_SHADOW_MONITORING"
        if not reasons:
            reasons.append("PAPER_ONLY_SIGNAL_LAYER_READY")
        if freshness_review["only_historical_static_review"]:
            reasons.append("HISTORICAL_STATIC_REVIEW_ONLY_NOT_LIVE_FRESHNESS")
    else:
        operator_state = "PAPER_REVIEW"
        status = "REVIEW"
        next_action = "INSPECT_FRESHNESS"
        reasons.append("OPERATOR_REVIEW_REQUIRED")

    freshness_section = {
        "status": freshness.get("status"),
        "flags": freshness_review["flags"],
        "live_review_flags": freshness_review["live_review_flags"],
        "historical_metric_static_review": freshness_review["historical_metric_static_review"],
        "only_historical_static_review": freshness_review["only_historical_static_review"],
        "decisions_total": freshness.get("decisions_total"),
        "candidates_this_cycle": freshness.get("candidates_this_cycle"),
        "new_snapshots_appended": freshness.get("new_snapshots_appended"),
    }
    paper_counts = {
        "total_paper_signals": paper_export.get("paper_signals_total") or paper_export.get("total_decisions") or 0,
        "paper_alert_ranker_status": ranker.get("status"),
        "operator_paper_decision_sheet_status": decision_sheet.get("status"),
        "ranked_alert_count": ranked_alert_count,
        "raw_ranked_alert_count": ranker.get("raw_ranked_alert_count") or ranked_alert_count,
        "grouped_ranked_alert_count": ranker.get("grouped_ranked_alert_count") or top_ranked_alert_count,
        "top_ranked_alert_count": top_ranked_alert_count,
        "new_ranked_alert_count": ranker.get("new_ranked_alert_count") or 0,
        "updated_ranked_alert_count": ranker.get("updated_ranked_alert_count") or 0,
        "repeated_ranked_alert_count": ranker.get("repeated_ranked_alert_count") or 0,
        "new_paper_alerts": dedupe.get("new_alerts") or 0,
        "raw_new_paper_alerts": dedupe.get("raw_new_alerts") or dedupe.get("new_alerts") or 0,
        "new_canonical_alerts": dedupe.get("new_canonical_alerts") or 0,
        "updated_canonical_alerts": dedupe.get("updated_canonical_alerts") or 0,
        "repeated_canonical_alerts": dedupe.get("repeated_canonical_alerts") or 0,
        "material_updates": dedupe.get("material_updates") or 0,
        "repeated_paper_alerts": dedupe.get("repeated_alerts") or 0,
        "sendable_discord_payload": discord_payload.get("sendable") is True,
        "clv_tracker_status": clv_tracker.get("status") or ("MISSING" if clv_tracker.get("missing") else "UNKNOWN"),
        "calibration_status": calibration.get("status") or ("MISSING" if calibration.get("missing") else "UNKNOWN"),
        "promotion_policy_status": promotion_policy.get("status") or ("MISSING" if promotion_policy.get("missing") else "UNKNOWN"),
        "promotion_policy_verdict": promotion_policy.get("final_verdict") or "UNKNOWN",
    }
    monitor_info = monitor_section(monitor, full_cycle, generated_at_utc)

    return {
        "mode": "FQIS_OPERATOR_SHADOW_CONSOLE",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "operator_console_generated_at_utc": generated_at_utc,
        "monitor_artifact_generated_at_utc": monitor_info.get("monitor_artifact_generated_at_utc"),
        "operator_state": operator_state,
        "next_action": next_action,
        "reasons": reasons,
        "safety": safety,
        "unsafe_flag_paths": unsafe_hits,
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "full_cycle_status": full_cycle.get("status"),
        "paper_counts": paper_counts,
        "freshness": freshness_section,
        "live_opportunity_scanner": live_opportunity_scanner_section(live_opportunity_scanner),
        "level3_stats_coverage_diagnostic": level3_stats_coverage_diagnostic_section(level3_stats_coverage_diagnostic),
        "paper_alert_ranker": {
            "status": ranker.get("status"),
            "ranked_alert_count": ranked_alert_count,
            "raw_ranked_alert_count": ranker.get("raw_ranked_alert_count") or ranked_alert_count,
            "grouped_ranked_alert_count": ranker.get("grouped_ranked_alert_count") or top_ranked_alert_count,
            "top_ranked_alert_count": top_ranked_alert_count,
            "new_ranked_alert_count": ranker.get("new_ranked_alert_count") or 0,
            "updated_ranked_alert_count": ranker.get("updated_ranked_alert_count") or 0,
            "repeated_ranked_alert_count": ranker.get("repeated_ranked_alert_count") or 0,
        },
        "clv_tracker": {
            "status": paper_counts["clv_tracker_status"],
            "total_records": clv_tracker.get("total_records", 0),
            "eligible_records": clv_tracker.get("eligible_records", 0),
            "favorable_move_rate": clv_tracker.get("favorable_move_rate"),
            "warning_flags": clv_tracker.get("warning_flags") or [],
        },
        "calibration": {
            "status": paper_counts["calibration_status"],
            "total_rows": calibration.get("total_rows", 0),
            "eligible_settled_rows": calibration.get("eligible_settled_rows", 0),
            "brier_score": calibration.get("brier_score"),
            "log_loss": calibration.get("log_loss"),
            "warning_flags": calibration.get("warning_flags") or [],
        },
        "promotion_policy": {
            "status": paper_counts["promotion_policy_status"],
            "final_verdict": paper_counts["promotion_policy_verdict"],
            "promotion_allowed": promotion_policy.get("promotion_allowed") is True,
            "promotion_allowed_count": promotion_policy.get("promotion_allowed_count", 0),
            "warning_flags": promotion_policy.get("warning_flags") or [],
        },
        "operator_paper_decision_sheet": {
            "status": decision_sheet.get("status"),
            "top_ranked_alert_count": decision_sheet.get("top_ranked_alert_count") or 0,
        },
        "monitor": monitor_info,
        "discord": {
            "status": discord_payload.get("status"),
            "sendable": discord_payload.get("sendable") is True,
            "send_reason": discord_payload.get("send_reason"),
        },
        "digest": {
            "status": digest.get("status"),
            "verdict": digest.get("verdict"),
        },
        **safety,
        **paper_counts,
    }


def safe_text(value: Any) -> str:
    return str(value or "").replace("\n", " ").strip()


def render_reason_counts(reason_counts: Any) -> str:
    if not reason_counts:
        return "NONE"
    parts = []
    for item in reason_counts:
        if not isinstance(item, dict):
            continue
        parts.append(f"{safe_text(item.get('reason'))}={safe_int(item.get('count'))}")
    return "; ".join(parts) if parts else "NONE"


def write_markdown(payload: dict[str, Any]) -> None:
    safety = payload.get("safety") or {}
    freshness = payload.get("freshness") or {}
    live_opportunity_scanner = payload.get("live_opportunity_scanner") or {}
    level3_stats_coverage_diagnostic = payload.get("level3_stats_coverage_diagnostic") or {}
    paper_counts = payload.get("paper_counts") or {}
    clv_tracker = payload.get("clv_tracker") or {}
    calibration = payload.get("calibration") or {}
    promotion_policy = payload.get("promotion_policy") or {}
    discord = payload.get("discord") or {}
    monitor = payload.get("monitor") or {}
    lines = [
        "# FQIS Operator Shadow Console",
        "",
        "## Safety State",
        "",
        f"- Status: **{payload.get('status')}**",
        f"- Operator state: **{payload.get('operator_state')}**",
        f"- Next action: **{payload.get('next_action')}**",
        f"- Promotion allowed: **{safety.get('promotion_allowed')}**",
        f"- Live staking allowed: **{safety.get('live_staking_allowed')}**",
        f"- Can execute real bets: **{safety.get('can_execute_real_bets')}**",
        f"- Can enable live staking: **{safety.get('can_enable_live_staking')}**",
        f"- Can mutate ledger: **{safety.get('can_mutate_ledger')}**",
        "",
        "## Shadow Readiness",
        "",
        f"- Full cycle: **{payload.get('full_cycle_status')}**",
        f"- Go/no-go: **{payload.get('go_no_go_state')}**",
        f"- Shadow: **{payload.get('shadow_state')}**",
        "",
        "## Live Freshness",
        "",
        f"- Status: **{freshness.get('status')}**",
        f"- Flags: **{', '.join(str(flag) for flag in freshness.get('flags') or []) or 'NONE'}**",
        f"- Live review flags: **{', '.join(str(flag) for flag in freshness.get('live_review_flags') or []) or 'NONE'}**",
        f"- Historical static review: **{', '.join(str(flag) for flag in freshness.get('historical_metric_static_review') or []) or 'NONE'}**",
        f"- Decisions total: **{freshness.get('decisions_total')}**",
        f"- Candidates this cycle: **{freshness.get('candidates_this_cycle')}**",
        f"- New snapshots appended: **{freshness.get('new_snapshots_appended')}**",
        "",
        "## Live Opportunity Scanner",
        "",
        f"- status: **{live_opportunity_scanner.get('status')}**",
        f"- operator_read: **{live_opportunity_scanner.get('operator_read')}**",
        f"- live_fixtures_seen: **{live_opportunity_scanner.get('live_fixtures_seen')}**",
        f"- groups_total / groups_priced: **{live_opportunity_scanner.get('groups_total')} / {live_opportunity_scanner.get('groups_priced')}**",
        f"- decisions_total: **{live_opportunity_scanner.get('decisions_total')}**",
        f"- candidates_this_cycle: **{live_opportunity_scanner.get('candidates_this_cycle')}**",
        f"- new_snapshots_appended: **{live_opportunity_scanner.get('new_snapshots_appended')}**",
        f"- level3_state_ready_count / level3_trade_ready_count: **{live_opportunity_scanner.get('level3_state_ready_count')} / {live_opportunity_scanner.get('level3_trade_ready_count')}**",
        f"- level3_events_available_count / level3_stats_available_count: **{live_opportunity_scanner.get('level3_events_available_count')} / {live_opportunity_scanner.get('level3_stats_available_count')}**",
        f"- score_only_decisions: **{live_opportunity_scanner.get('score_only_decisions')}**",
        f"- rejected_by_non_positive_edge_ev: **{live_opportunity_scanner.get('rejected_by_non_positive_edge_ev')}**",
        f"- rejected_by_timing_policy: **{live_opportunity_scanner.get('rejected_by_timing_policy')}**",
        f"- rejected_by_data_tier: **{live_opportunity_scanner.get('rejected_by_data_tier')}**",
        f"- rejected_by_final_status: **{live_opportunity_scanner.get('rejected_by_final_status')}**",
        f"- rejected_by_negative_value_veto: **{live_opportunity_scanner.get('rejected_by_negative_value_veto')}**",
        "",
        "### Top 3 Rejection Reasons",
        "",
        "## Level 3 Stats Coverage Diagnostic",
        "",
        f"- status: **{level3_stats_coverage_diagnostic.get('status')}**",
        f"- fixtures_seen: **{level3_stats_coverage_diagnostic.get('fixtures_seen')}**",
        f"- events_available: **{level3_stats_coverage_diagnostic.get('events_available')}**",
        f"- raw_stats_available: **{level3_stats_coverage_diagnostic.get('raw_stats_available')}**",
        f"- parsed_stats_available: **{level3_stats_coverage_diagnostic.get('parsed_stats_available')}**",
        f"- events_only_no_stats: **{level3_stats_coverage_diagnostic.get('events_only_no_stats')}**",
        f"- stats_parser_empty: **{level3_stats_coverage_diagnostic.get('stats_parser_empty')}**",
        f"- stats_endpoint_missing: **{level3_stats_coverage_diagnostic.get('stats_endpoint_missing')}**",
        f"- reason_counts: **{render_reason_counts(level3_stats_coverage_diagnostic.get('reason_counts'))}**",
        "",
        "## Paper Signals",
        "",
        f"- Total paper signals: **{paper_counts.get('total_paper_signals')}**",
        f"- Paper alert ranker status: **{paper_counts.get('paper_alert_ranker_status')}**",
        f"- Decision sheet status: **{paper_counts.get('operator_paper_decision_sheet_status')}**",
        f"- Ranked alerts: **{paper_counts.get('ranked_alert_count')}**",
        f"- Top ranked alerts: **{paper_counts.get('top_ranked_alert_count')}**",
        f"- New ranked alerts: **{paper_counts.get('new_ranked_alert_count')}**",
        f"- Repeated ranked alerts: **{paper_counts.get('repeated_ranked_alert_count')}**",
        f"- New paper alerts: **{paper_counts.get('new_paper_alerts')}**",
        f"- Repeated paper alerts: **{paper_counts.get('repeated_paper_alerts')}**",
        "",
        "## Proxy CLV / Calibration / Promotion",
        "",
        f"- Proxy CLV status: **{clv_tracker.get('status')}**",
        f"- Proxy CLV eligible records: **{clv_tracker.get('eligible_records')}**",
        f"- Proxy CLV favorable move rate: **{clv_tracker.get('favorable_move_rate')}**",
        f"- Calibration status: **{calibration.get('status')}**",
        f"- Calibration eligible settled rows: **{calibration.get('eligible_settled_rows')}**",
        f"- Calibration Brier score: **{calibration.get('brier_score')}**",
        f"- Promotion policy status: **{promotion_policy.get('status')}**",
        f"- Promotion policy verdict: **{promotion_policy.get('final_verdict')}**",
        f"- Promotion policy allowed: **{promotion_policy.get('promotion_allowed')}**",
        "",
        "## Discord Paper Payload",
        "",
        f"- Status: **{discord.get('status')}**",
        f"- Sendable: **{discord.get('sendable')}**",
        f"- Send reason: **{discord.get('send_reason')}**",
        "",
        "## Monitor Summary",
        "",
        f"- Context: **{monitor.get('monitor_context')}**",
        f"- Monitor artifact generated at UTC: `{monitor.get('monitor_artifact_generated_at_utc')}`",
        f"- Operator console generated at UTC: `{monitor.get('operator_console_generated_at_utc')}`",
        f"- Cycles completed: **{monitor.get('cycles_completed')}**",
        f"- Cycles requested: **{monitor.get('cycles_requested')}**",
        f"- Stopped reason: **{monitor.get('stopped_reason') or 'NONE'}**",
        f"- All ledger preserved: **{monitor.get('all_ledger_preserved')}**",
        f"- Any real bets enabled: **{monitor.get('any_real_bets_enabled')}**",
        f"- Any live staking enabled: **{monitor.get('any_live_staking_enabled')}**",
        f"- Any promotion allowed: **{monitor.get('any_promotion_allowed')}**",
        "",
        "## Red Lines",
        "",
        "- Do not bet real money.",
        "- Do not enable live staking.",
        "- Do not mutate the research ledger.",
        "- Do not interpret paper alerts as betting instructions.",
        "",
        "## Next Operator Action",
        "",
        f"- **{payload.get('next_action')}**",
        "",
        "## Reasons",
        "",
    ]
    top_rejection_reasons = live_opportunity_scanner.get("top_rejection_reasons") or []
    if top_rejection_reasons:
        insert_at = lines.index("## Level 3 Stats Coverage Diagnostic") - 1
        for item in reversed(top_rejection_reasons[:3]):
            lines.insert(insert_at, f"- {item.get('count', 0)}: {safe_text(item.get('reason'))}")
    else:
        insert_at = lines.index("## Level 3 Stats Coverage Diagnostic") - 1
        lines.insert(insert_at, "- NONE")
    for reason in payload.get("reasons") or []:
        lines.append(f"- {safe_text(reason)}")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(payload: dict[str, Any]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)


def main() -> int:
    payload = build_payload()
    write_outputs(payload)
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0 if payload["status"] in {"READY", "REVIEW"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
