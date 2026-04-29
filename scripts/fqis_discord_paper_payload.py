from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_READINESS_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
OUT_JSON = ORCH_DIR / "latest_discord_paper_payload.json"
OUT_MD = ORCH_DIR / "latest_discord_paper_payload.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
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


def safe_text(value: Any) -> str:
    return str(value or "").replace("\n", " ").strip()


def build_alert_lines(records: list[dict[str, Any]], max_alerts: int = 10) -> list[str]:
    lines: list[str] = []
    for index, record in enumerate(records[:max_alerts], start=1):
        lines.append(
            "{index}. {match} {minute}' {score} | {selection} @ {odds} | edge {edge} | EV {ev} | {paper_action}".format(
                index=index,
                match=safe_text(record.get("match") or record.get("fixture_id")),
                minute=safe_text(record.get("minute")),
                score=safe_text(record.get("score")),
                selection=safe_text(record.get("selection")),
                odds=safe_text(record.get("odds")),
                edge=safe_text(record.get("edge_prob")),
                ev=safe_text(record.get("ev_real")),
                paper_action=safe_text(record.get("paper_action")),
            )
        )
    return lines


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_READINESS_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)

    inputs = {
        "paper_signal_export": export,
        "paper_alert_dedupe": dedupe,
        "go_no_go": go_no_go,
        "shadow_readiness": shadow,
        "live_freshness": freshness,
    }

    reasons: list[str] = []
    for name, payload in inputs.items():
        if payload.get("missing") or payload.get("error"):
            reasons.append(f"MISSING_INPUT:{name}")

    if export.get("status") == "BLOCKED":
        reasons.append("PAPER_SIGNAL_EXPORT_BLOCKED")
    if dedupe.get("status") == "BLOCKED":
        reasons.append("PAPER_ALERT_DEDUPE_BLOCKED")
    if go_no_go.get("status") != "READY":
        reasons.append("GO_NO_GO_NOT_READY")
    if shadow.get("status") != "READY" or shadow.get("shadow_state") != "SHADOW_READY":
        reasons.append("SHADOW_READINESS_NOT_READY")
    if freshness.get("status") == "MISSING_INPUTS":
        reasons.append("LIVE_FRESHNESS_MISSING_INPUTS")

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
    if unsafe_hits:
        reasons.append("UNSAFE_TRUE_FLAGS:" + ",".join(unsafe_hits[:20]))

    new_records = dedupe.get("new_alert_records") or []
    if not isinstance(new_records, list):
        new_records = []

    alert_lines = build_alert_lines([r for r in new_records if isinstance(r, dict)])
    dashboard = {
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "freshness_status": freshness.get("status"),
        "decisions_total": freshness.get("decisions_total") or export.get("total_decisions"),
        "candidates_this_cycle": freshness.get("candidates_this_cycle"),
        "new_snapshots_appended": freshness.get("new_snapshots_appended"),
        "post_quarantine_pnl": (shadow.get("post_quarantine") or {}).get("pnl"),
        "post_quarantine_roi": (shadow.get("post_quarantine") or {}).get("roi"),
        "unsafe_flags": unsafe_hits,
    }

    status = "BLOCKED" if reasons else "READY"
    sendable = status == "READY" and bool(alert_lines)
    if status == "BLOCKED":
        send_reason = "BLOCKED_BY_SAFETY_OR_INPUTS"
    elif not alert_lines:
        send_reason = "NO_NEW_PAPER_ALERTS"
    else:
        send_reason = "NEW_PAPER_ALERTS_READY"

    body_lines = [
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        f"Go/No-Go: {dashboard.get('go_no_go_state')} | Shadow: {dashboard.get('shadow_state')} | Freshness: {dashboard.get('freshness_status')}",
        f"Decisions: {dashboard.get('decisions_total')} | Candidates: {dashboard.get('candidates_this_cycle')} | New snapshots: {dashboard.get('new_snapshots_appended')}",
        f"Post-quarantine PnL/ROI: {dashboard.get('post_quarantine_pnl')} / {dashboard.get('post_quarantine_roi')}",
    ]
    if alert_lines:
        body_lines += ["", *alert_lines]
    else:
        body_lines += ["", "No new paper alerts."]

    body = "\n".join(body_lines)
    return {
        "mode": "FQIS_DISCORD_PAPER_PAYLOAD",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "reasons": reasons,
        "sendable": sendable,
        "send_reason": send_reason,
        "paper_only": True,
        "discord_send_performed": False,
        "dashboard": dashboard,
        "alerts_included": len(alert_lines),
        "alerts_cap": 10,
        "alert_records": new_records[:10],
        "content": body,
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
    }


def write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# FQIS Discord Paper Payload",
        "",
        f"- status: **{payload.get('status')}**",
        f"- sendable: **{payload.get('sendable')}**",
        f"- send_reason: **{payload.get('send_reason')}**",
        f"- discord_send_performed: **{payload.get('discord_send_performed')}**",
        "",
        "```text",
        payload.get("content") or "",
        "```",
    ]
    if payload.get("reasons"):
        lines += ["", "## Block Reasons", ""]
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
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
