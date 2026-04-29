from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
OUT_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
OUT_MD = ORCH_DIR / "latest_paper_alert_dedupe.md"
STATE_JSON = ORCH_DIR / "paper_alert_state.json"

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


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def stable_alert_key(signal: dict[str, Any]) -> str:
    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    rounded_odds = "" if odds is None else f"{odds:.3f}"
    parts = [
        signal.get("fixture_id") or "",
        signal.get("selection") or "",
        signal.get("market") or "",
        signal.get("final_pipeline") or "",
        rounded_odds,
    ]
    return "|".join(str(part).strip().lower() for part in parts)


def load_state() -> dict[str, Any]:
    payload = read_json(STATE_JSON)
    if payload.get("missing") or payload.get("error"):
        return {"version": 1, "seen_alert_keys": {}}
    seen = payload.get("seen_alert_keys")
    if isinstance(seen, list):
        seen = {str(key): {"first_seen_utc": "", "last_seen_utc": "", "count": 1} for key in seen}
    if not isinstance(seen, dict):
        seen = {}
    return {"version": 1, "seen_alert_keys": seen}


def alert_record(signal: dict[str, Any], alert_key: str, repeated: bool) -> dict[str, Any]:
    return {
        "alert_key": alert_key,
        "repeated": repeated,
        "fixture_id": signal.get("fixture_id"),
        "match": signal.get("match"),
        "minute": signal.get("minute"),
        "score": signal.get("score"),
        "market": signal.get("market"),
        "selection": signal.get("selection"),
        "odds": signal.get("odds"),
        "edge_prob": signal.get("edge_prob"),
        "ev_real": signal.get("ev_real"),
        "final_pipeline": signal.get("final_pipeline"),
        "paper_action": signal.get("paper_action"),
    }


def is_alertable_signal(signal: dict[str, Any]) -> bool:
    if signal.get("paper_action") == "PAPER_REJECTED_NO_ACTION":
        return False
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))
    if ev is not None and ev <= 0:
        return False
    if edge is not None and edge <= 0:
        return False
    return True


def build_payload() -> tuple[dict[str, Any], dict[str, Any] | None]:
    generated_at_utc = utc_now()
    export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    state = load_state()
    seen: dict[str, Any] = state["seen_alert_keys"]
    state_before_size = len(seen)

    reasons: list[str] = []
    if export.get("missing") or export.get("error"):
        reasons.append("MISSING_PAPER_SIGNAL_EXPORT")
    if export.get("status") == "BLOCKED":
        reasons.append("PAPER_SIGNAL_EXPORT_BLOCKED")

    signals = export.get("signals") or []
    if not isinstance(signals, list):
        signals = []
        reasons.append("PAPER_SIGNALS_NOT_LIST")

    if reasons:
        payload = {
            "mode": "FQIS_PAPER_ALERT_DEDUPE",
            "status": "BLOCKED",
            "generated_at_utc": generated_at_utc,
            "reasons": reasons,
            "safety": dict(SAFETY_BLOCK),
            "total_signals": len(signals),
            "new_alerts": 0,
            "repeated_alerts": 0,
            "suppressed_repeats": 0,
            "state_size": state_before_size,
            "new_alert_records": [],
            "repeated_alert_records": [],
            **SAFETY_BLOCK,
        }
        return payload, None

    new_records: list[dict[str, Any]] = []
    repeated_records: list[dict[str, Any]] = []
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        if not is_alertable_signal(signal):
            continue
        alert_key = stable_alert_key(signal)
        if not alert_key.strip("|"):
            continue
        if alert_key in seen:
            record = alert_record(signal, alert_key, repeated=True)
            repeated_records.append(record)
            seen_record = seen.get(alert_key) if isinstance(seen.get(alert_key), dict) else {}
            seen_record["last_seen_utc"] = generated_at_utc
            seen_record["count"] = int(seen_record.get("count") or 1) + 1
            seen[alert_key] = seen_record
        else:
            record = alert_record(signal, alert_key, repeated=False)
            new_records.append(record)
            seen[alert_key] = {
                "first_seen_utc": generated_at_utc,
                "last_seen_utc": generated_at_utc,
                "count": 1,
                "fixture_id": signal.get("fixture_id"),
                "selection": signal.get("selection"),
                "market": signal.get("market"),
                "final_pipeline": signal.get("final_pipeline"),
                "odds": signal.get("odds"),
            }

    next_state = {
        "version": 1,
        "updated_at_utc": generated_at_utc,
        "seen_alert_keys": seen,
    }
    payload = {
        "mode": "FQIS_PAPER_ALERT_DEDUPE",
        "status": "READY",
        "generated_at_utc": generated_at_utc,
        "reasons": [],
        "safety": dict(SAFETY_BLOCK),
        "total_signals": len(signals),
        "new_alerts": len(new_records),
        "repeated_alerts": len(repeated_records),
        "suppressed_repeats": len(repeated_records),
        "state_size": len(seen),
        "state_size_before": state_before_size,
        "state_file": str(STATE_JSON),
        "new_alert_records": new_records,
        "repeated_alert_records": repeated_records,
        **SAFETY_BLOCK,
    }
    return payload, next_state


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


def write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# FQIS Paper Alert Dedupe",
        "",
        f"- status: **{payload.get('status')}**",
        f"- generated_at_utc: `{payload.get('generated_at_utc')}`",
        f"- total_signals: **{payload.get('total_signals', 0)}**",
        f"- new_alerts: **{payload.get('new_alerts', 0)}**",
        f"- repeated_alerts: **{payload.get('repeated_alerts', 0)}**",
        f"- suppressed_repeats: **{payload.get('suppressed_repeats', 0)}**",
        f"- state_size: **{payload.get('state_size', 0)}**",
        "- can_execute_real_bets: **False**",
        "- can_enable_live_staking: **False**",
        "- can_mutate_ledger: **False**",
        "",
        "## New Paper Alerts",
        "",
    ]
    records = payload.get("new_alert_records") or []
    if not records:
        lines.append("- None.")
    else:
        for record in records[:25]:
            lines.append(
                "- {match} | {minute}' | {selection} | {odds} | {paper_action}".format(
                    match=safe_text(record.get("match") or record.get("fixture_id")),
                    minute=safe_text(record.get("minute")),
                    selection=safe_text(record.get("selection")),
                    odds=safe_text(record.get("odds")),
                    paper_action=safe_text(record.get("paper_action")),
                )
            )
    if payload.get("reasons"):
        lines += ["", "## Block Reasons", ""]
        lines.extend(f"- {safe_text(reason)}" for reason in payload.get("reasons") or [])

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(payload: dict[str, Any], next_state: dict[str, Any] | None) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    if next_state is not None:
        STATE_JSON.write_text(json.dumps(next_state, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)


def main() -> int:
    payload, next_state = build_payload()
    write_outputs(payload, next_state)
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
