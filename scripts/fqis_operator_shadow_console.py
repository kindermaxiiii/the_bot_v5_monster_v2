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
PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
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


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    full_cycle = read_json(FULL_CYCLE_JSON)
    reports = full_cycle.get("reports") or {}
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_READINESS_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)
    paper_export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    ranker = read_json(PAPER_ALERT_RANKER_JSON)
    decision_sheet = read_json(OPERATOR_PAPER_DECISION_SHEET_JSON)
    discord_payload = read_json(DISCORD_PAPER_PAYLOAD_JSON)
    monitor = read_json(TONIGHT_MONITOR_JSON) if TONIGHT_MONITOR_JSON.exists() else {}
    digest = read_json(TONIGHT_DIGEST_JSON) if TONIGHT_DIGEST_JSON.exists() else {}

    daily_verdict = (reports.get("daily_audit") or {}).get("verdict") or {}
    invariants = full_cycle.get("invariants") or {}

    safety = {
        "promotion_allowed": safe_bool(go_no_go.get("promotion_allowed"), daily_verdict.get("promotion_allowed")),
        "live_staking_allowed": safe_bool(
            go_no_go.get("live_staking_allowed"),
            invariants.get("live_staking_enabled"),
            (paper_export.get("safety") or {}).get("live_staking_allowed"),
        ),
        "can_execute_real_bets": safe_bool(
            shadow.get("can_execute_real_bets"),
            paper_export.get("can_execute_real_bets"),
            ranker.get("can_execute_real_bets"),
            decision_sheet.get("can_execute_real_bets"),
            discord_payload.get("can_execute_real_bets"),
        ),
        "can_enable_live_staking": safe_bool(
            shadow.get("can_enable_live_staking"),
            paper_export.get("can_enable_live_staking"),
            ranker.get("can_enable_live_staking"),
            decision_sheet.get("can_enable_live_staking"),
            discord_payload.get("can_enable_live_staking"),
        ),
        "can_mutate_ledger": safe_bool(
            shadow.get("can_mutate_ledger"),
            paper_export.get("can_mutate_ledger"),
            dedupe.get("can_mutate_ledger"),
            ranker.get("can_mutate_ledger"),
            decision_sheet.get("can_mutate_ledger"),
            discord_payload.get("can_mutate_ledger"),
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
    unsafe_names = {
        "live_staking_allowed",
        "level3_live_staking_allowed",
        "can_execute_real_bets",
        "can_enable_live_staking",
        "can_mutate_ledger",
        "promotion_allowed",
    }
    unsafe_hits: list[str] = []
    for name, payload in inputs.items():
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


def write_markdown(payload: dict[str, Any]) -> None:
    safety = payload.get("safety") or {}
    freshness = payload.get("freshness") or {}
    paper_counts = payload.get("paper_counts") or {}
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
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] in {"READY", "REVIEW"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
