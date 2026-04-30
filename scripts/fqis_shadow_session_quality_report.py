from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

MONITOR_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
DIGEST_JSON = ORCH_DIR / "latest_tonight_shadow_digest.json"
OPERATOR_CONSOLE_JSON = ORCH_DIR / "latest_operator_shadow_console.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
OUT_JSON = ORCH_DIR / "latest_shadow_session_quality_report.json"
OUT_MD = ORCH_DIR / "latest_shadow_session_quality_report.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}

HISTORICAL_STATIC_REVIEW_FLAGS = {
    "CONSTANT_POST_QUARANTINE_PNL_REVIEW",
    "CONSTANT_FIXTURE_PNL_REVIEW",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
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


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = fnum(row.get(key))
        if value is not None:
            values.append(value)
    return values


def stat_block(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = numeric_values(rows, key)
    if not values:
        return {"min": None, "max": None, "avg": None}
    return {
        "min": min(values),
        "max": max(values),
        "avg": round(sum(values) / len(values), 6),
    }


def cycle_ranges(cycles: list[int]) -> list[str]:
    if not cycles:
        return []
    ranges: list[str] = []
    start = cycles[0]
    previous = cycles[0]
    for cycle in cycles[1:]:
        if cycle == previous + 1:
            previous = cycle
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = cycle
        previous = cycle
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ranges


def live_review_flags(freshness_flags: list[Any], historical_flags: list[Any]) -> list[str]:
    historical = {str(flag) for flag in historical_flags}
    historical.update(flag for flag in HISTORICAL_STATIC_REVIEW_FLAGS if flag in {str(item) for item in freshness_flags})
    return [
        str(flag)
        for flag in freshness_flags
        if str(flag) != "OK_FRESH_LIVE_CYCLE"
        and str(flag) not in HISTORICAL_STATIC_REVIEW_FLAGS
        and str(flag) not in historical
    ]


def safe_text(value: Any) -> str:
    return str(value or "").replace("\n", " ").strip()


def recommended_next_action_for(
    *,
    quality_state: str,
    unsafe: bool,
    monitor_stopped: bool,
    zero_decision_cycles: int,
    live_review_flags_final: list[str],
) -> str:
    if unsafe or monitor_stopped:
        return "STOP_SESSION_AND_INSPECT_SAFETY"
    has_zero_decisions = zero_decision_cycles > 0
    has_live_review_flags = bool(live_review_flags_final)
    if has_zero_decisions and has_live_review_flags:
        return "REVIEW_ZERO_DECISIONS_AND_FRESHNESS"
    if has_zero_decisions:
        return "REVIEW_ZERO_DECISION_CYCLES"
    if has_live_review_flags:
        return "REVIEW_FRESHNESS_FLAGS"
    if quality_state == "SESSION_GREEN":
        return "CONTINUE_PAPER_SHADOW_MONITORING"
    return "REVIEW_SESSION_CONTEXT"


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    monitor = read_json(MONITOR_JSON)
    digest = read_json(DIGEST_JSON)
    operator = read_json(OPERATOR_CONSOLE_JSON)
    ranker = read_json(PAPER_ALERT_RANKER_JSON)
    dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)
    full_cycle = read_json(FULL_CYCLE_JSON)

    inputs = {
        "latest_tonight_shadow_monitor": monitor,
        "latest_tonight_shadow_digest": digest,
        "latest_operator_shadow_console": operator,
        "latest_paper_alert_ranker": ranker,
        "latest_paper_alert_dedupe": dedupe,
        "latest_live_freshness_report": freshness,
        "latest_full_cycle_report": full_cycle,
    }

    if monitor.get("missing") or monitor.get("error"):
        return {
            "mode": "FQIS_SHADOW_SESSION_QUALITY_REPORT",
            "status": "NO_MONITOR_SESSION_AVAILABLE",
            "quality_state": "NO_MONITOR_SESSION_AVAILABLE",
            "generated_at_utc": generated_at_utc,
            "reasons": ["NO_MONITOR_SESSION_AVAILABLE"],
            "source_files": {name: payload.get("path") for name, payload in inputs.items() if isinstance(payload, dict)},
            "cycles_completed": 0,
            "final_verdict": "NO_MONITOR_SESSION_AVAILABLE",
            "recommended_next_action": "RUN_SHADOW_MONITOR",
            "safety_flags": dict(SAFETY_BLOCK),
            **SAFETY_BLOCK,
        }

    rows = monitor.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    rows = [row for row in rows if isinstance(row, dict)]
    summary = monitor.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}

    unsafe_names = {
        "live_staking_allowed",
        "level3_live_staking_allowed",
        "can_execute_real_bets",
        "can_enable_live_staking",
        "can_mutate_ledger",
        "promotion_allowed",
    }
    unsafe_paths: list[str] = []
    for name, payload in inputs.items():
        unsafe_paths.extend(f"{name}:{path}" for path in find_true_flags(payload, unsafe_names))

    cycles_completed = int(monitor.get("cycles_completed") or len(rows))
    ready_cycles = sum(1 for row in rows if row.get("full_cycle_status") == "READY")
    shadow_ready_cycles = sum(1 for row in rows if row.get("shadow_state") == "SHADOW_READY")
    paper_ready_cycles = sum(1 for row in rows if row.get("operator_state") == "PAPER_READY")
    paper_review_cycles = sum(1 for row in rows if row.get("operator_state") == "PAPER_REVIEW")
    stale_review_cycles = sum(1 for row in rows if row.get("live_freshness_status") == "STALE_REVIEW")
    zero_decision_cycle_numbers = [
        int(row.get("cycle") or index + 1)
        for index, row in enumerate(rows)
        if int(row.get("decisions_total") or 0) == 0
    ]

    total_raw_new_paper_alerts = sum(
        int(row.get("raw_new_paper_alerts") or row.get("new_paper_alerts") or 0)
        for row in rows
    )
    if not rows:
        total_raw_new_paper_alerts = int(dedupe.get("raw_new_alerts") or dedupe.get("new_alerts") or 0)

    total_canonical_new_alerts = sum(int(row.get("new_canonical_alerts") or 0) for row in rows)
    if total_canonical_new_alerts == 0 and not rows:
        total_canonical_new_alerts = int(dedupe.get("new_canonical_alerts") or dedupe.get("new_alerts") or 0)

    total_material_updates = sum(int(row.get("material_updates") or 0) for row in rows)
    if total_material_updates == 0 and not rows:
        total_material_updates = int(dedupe.get("material_updates") or 0)

    freshness_flags = (
        digest.get("freshness_flags_final")
        or freshness.get("freshness_flags")
        or []
    )
    historical_static_review = (
        digest.get("historical_static_review_final")
        or freshness.get("historical_metric_static_review")
        or []
    )
    live_flags = live_review_flags(freshness_flags, historical_static_review)

    all_ledger_preserved = (
        summary.get("all_ledger_preserved") is True
        if "all_ledger_preserved" in summary
        else bool(rows) and all(row.get("ledger_preserved") is True for row in rows)
    )
    ledger_preserved_final = digest.get("ledger_preserved_final")
    if ledger_preserved_final is None and rows:
        ledger_preserved_final = rows[-1].get("ledger_preserved")

    safety_flags = {
        "all_ledger_preserved": all_ledger_preserved,
        "ledger_preserved_final": ledger_preserved_final,
        "any_real_bets_enabled": summary.get("any_real_bets_enabled") is True
        or digest.get("any_real_bets_enabled") is True,
        "any_live_staking_enabled": summary.get("any_live_staking_enabled") is True
        or digest.get("any_live_staking_enabled") is True,
        "any_promotion_allowed": summary.get("any_promotion_allowed") is True
        or digest.get("any_promotion_allowed") is True,
        "can_execute_real_bets": operator.get("can_execute_real_bets") is True,
        "can_enable_live_staking": operator.get("can_enable_live_staking") is True,
        "can_mutate_ledger": operator.get("can_mutate_ledger") is True,
        "live_staking_allowed": operator.get("live_staking_allowed") is True,
        "promotion_allowed": operator.get("promotion_allowed") is True,
        "unsafe_flag_paths": unsafe_paths,
    }
    unsafe = (
        bool(unsafe_paths)
        or safety_flags["all_ledger_preserved"] is not True
        or safety_flags["ledger_preserved_final"] is not True
        or safety_flags["any_real_bets_enabled"] is True
        or safety_flags["any_live_staking_enabled"] is True
        or safety_flags["any_promotion_allowed"] is True
        or safety_flags["can_execute_real_bets"] is True
        or safety_flags["can_enable_live_staking"] is True
        or safety_flags["can_mutate_ledger"] is True
        or safety_flags["live_staking_allowed"] is True
        or safety_flags["promotion_allowed"] is True
    )
    monitor_stopped = monitor.get("status") == "STOPPED"
    full_cycle_ready = (
        digest.get("final_full_cycle_status") == "READY"
        or full_cycle.get("status") == "READY"
        or (cycles_completed > 0 and ready_cycles == cycles_completed)
    )
    full_cycle_all_ready = cycles_completed > 0 and ready_cycles == cycles_completed

    if monitor_stopped or unsafe:
        quality_state = "SESSION_BLOCKED"
    elif zero_decision_cycle_numbers or live_flags:
        quality_state = "SESSION_REVIEW"
    elif full_cycle_ready and full_cycle_all_ready and all_ledger_preserved and not live_flags:
        quality_state = "SESSION_GREEN"
    else:
        quality_state = "SESSION_REVIEW"

    recommended_next_action = recommended_next_action_for(
        quality_state=quality_state,
        unsafe=unsafe,
        monitor_stopped=monitor_stopped,
        zero_decision_cycles=len(zero_decision_cycle_numbers),
        live_review_flags_final=live_flags,
    )

    total_sendable_canonical_events = total_canonical_new_alerts + total_material_updates
    raw_to_canonical_new_ratio = round(total_raw_new_paper_alerts / max(1, total_canonical_new_alerts), 6)
    raw_to_sendable_canonical_ratio = round(
        total_raw_new_paper_alerts / max(1, total_sendable_canonical_events),
        6,
    )
    alert_noise_ratio = raw_to_canonical_new_ratio

    payload = {
        "mode": "FQIS_SHADOW_SESSION_QUALITY_REPORT",
        "status": "READY",
        "quality_state": quality_state,
        "generated_at_utc": generated_at_utc,
        "source_files": {
            "latest_tonight_shadow_monitor": str(MONITOR_JSON),
            "latest_tonight_shadow_digest": str(DIGEST_JSON),
            "latest_operator_shadow_console": str(OPERATOR_CONSOLE_JSON),
            "latest_paper_alert_ranker": str(PAPER_ALERT_RANKER_JSON),
            "latest_paper_alert_dedupe": str(PAPER_ALERT_DEDUPE_JSON),
            "latest_live_freshness_report": str(LIVE_FRESHNESS_JSON),
        },
        "cycles_completed": cycles_completed,
        "ready_cycles": ready_cycles,
        "shadow_ready_cycles": shadow_ready_cycles,
        "paper_ready_cycles": paper_ready_cycles,
        "paper_review_cycles": paper_review_cycles,
        "stale_review_cycles": stale_review_cycles,
        "zero_decision_cycles": len(zero_decision_cycle_numbers),
        "zero_decision_cycle_numbers": zero_decision_cycle_numbers,
        "zero_decision_cycle_ranges": cycle_ranges(zero_decision_cycle_numbers),
        "total_new_snapshots_appended": int(summary.get("total_new_snapshots_appended") or sum(int(row.get("new_snapshots_appended") or 0) for row in rows)),
        "total_raw_new_paper_alerts": total_raw_new_paper_alerts,
        "total_canonical_new_alerts": total_canonical_new_alerts,
        "total_material_updates": total_material_updates,
        "total_sendable_canonical_events": total_sendable_canonical_events,
        "raw_to_canonical_new_ratio": raw_to_canonical_new_ratio,
        "raw_to_sendable_canonical_ratio": raw_to_sendable_canonical_ratio,
        "alert_noise_ratio": alert_noise_ratio,
        "decisions_total": stat_block(rows, "decisions_total"),
        "candidates_this_cycle": stat_block(rows, "candidates_this_cycle"),
        "ranked_alert_count": stat_block(rows, "ranked_alert_count"),
        "raw_ranked_alert_count": stat_block(rows, "raw_ranked_alert_count"),
        "grouped_ranked_alert_count": stat_block(rows, "grouped_ranked_alert_count"),
        "freshness_flags_final": freshness_flags,
        "historical_static_review_final": historical_static_review,
        "live_review_flags_final": live_flags,
        "safety_flags": safety_flags,
        "monitor_status": monitor.get("status"),
        "monitor_stopped": monitor_stopped,
        "full_cycle_ready": full_cycle_ready,
        "full_cycle_all_ready": full_cycle_all_ready,
        "final_verdict": quality_state,
        "digest_verdict": digest.get("verdict"),
        "recommended_next_action": recommended_next_action,
        "reasons": [],
        **SAFETY_BLOCK,
    }
    return payload


def write_markdown(payload: dict[str, Any]) -> None:
    safety = payload.get("safety_flags") or {}
    lines = [
        "# FQIS Shadow Session Quality Report",
        "",
        f"- Status: **{payload.get('status')}**",
        f"- Quality state: **{payload.get('quality_state')}**",
        f"- Generated at UTC: `{payload.get('generated_at_utc')}`",
        f"- Cycles completed: **{payload.get('cycles_completed', 0)}**",
        f"- Ready cycles: **{payload.get('ready_cycles', 0)}**",
        f"- Shadow ready cycles: **{payload.get('shadow_ready_cycles', 0)}**",
        f"- Paper ready cycles: **{payload.get('paper_ready_cycles', 0)}**",
        f"- Paper review cycles: **{payload.get('paper_review_cycles', 0)}**",
        f"- Stale review cycles: **{payload.get('stale_review_cycles', 0)}**",
        f"- Zero-decision cycles: **{payload.get('zero_decision_cycles', 0)}**",
        f"- Zero-decision ranges: **{', '.join(payload.get('zero_decision_cycle_ranges') or []) or 'NONE'}**",
        f"- Total new snapshots appended: **{payload.get('total_new_snapshots_appended', 0)}**",
        f"- Raw new paper alerts: **{payload.get('total_raw_new_paper_alerts', 0)}**",
        f"- Canonical new alerts: **{payload.get('total_canonical_new_alerts', 0)}**",
        f"- Material updates: **{payload.get('total_material_updates', 0)}**",
        f"- Sendable canonical events: **{payload.get('total_sendable_canonical_events', 0)}**",
        f"- Raw/canonical new ratio: **{payload.get('raw_to_canonical_new_ratio')}**",
        f"- Raw/sendable canonical ratio: **{payload.get('raw_to_sendable_canonical_ratio')}**",
        f"- Alert noise ratio: **{payload.get('alert_noise_ratio')}**",
        f"- Final verdict: **{payload.get('final_verdict')}**",
        f"- Recommended next action: **{payload.get('recommended_next_action')}**",
        "",
        "## Safety Flags",
        "",
        f"- Ledger preserved final: **{safety.get('ledger_preserved_final')}**",
        f"- All ledger preserved: **{safety.get('all_ledger_preserved')}**",
        f"- Any real bets enabled: **{safety.get('any_real_bets_enabled')}**",
        f"- Any live staking enabled: **{safety.get('any_live_staking_enabled')}**",
        f"- Any promotion allowed: **{safety.get('any_promotion_allowed')}**",
        f"- Can execute real bets: **{safety.get('can_execute_real_bets')}**",
        f"- Can enable live staking: **{safety.get('can_enable_live_staking')}**",
        f"- Can mutate ledger: **{safety.get('can_mutate_ledger')}**",
        f"- Live staking allowed: **{safety.get('live_staking_allowed')}**",
        f"- Promotion allowed: **{safety.get('promotion_allowed')}**",
        "",
        "## Freshness",
        "",
        f"- Live review flags final: **{', '.join(str(flag) for flag in payload.get('live_review_flags_final') or []) or 'NONE'}**",
        f"- Historical static review final: **{', '.join(str(flag) for flag in payload.get('historical_static_review_final') or []) or 'NONE'}**",
    ]
    if payload.get("reasons"):
        lines += ["", "## Reasons", ""]
        lines.extend(f"- {safe_text(reason)}" for reason in payload.get("reasons") or [])

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
