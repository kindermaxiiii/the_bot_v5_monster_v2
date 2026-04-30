from __future__ import annotations

import json
import os
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
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}

DEFAULT_MINUTE_BUCKET = 5
DEFAULT_ODDS_MATERIAL_THRESHOLD = 0.05
DEFAULT_EV_MATERIAL_THRESHOLD = 0.03
DEFAULT_EDGE_MATERIAL_THRESHOLD = 0.02

DATA_TIER_PRIORITY = {
    "UNKNOWN": 99,
    "EVENTS_ONLY_RESEARCH": 2,
    "STRICT_EVENTS_PLUS_STATS": 1,
}

PIPELINE_PRIORITY = {
    "research": 2,
    "production": 1,
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


def config_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except Exception:
        return default


def config_float(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.environ.get(name, default)))
    except Exception:
        return default


def normalized_data_tier(signal: dict[str, Any]) -> str:
    tier = str(signal.get("research_data_tier") or signal.get("data_tier") or "").strip()
    if tier in {"STRICT_EVENTS_PLUS_STATS", "EVENTS_PLUS_STATS"}:
        return "STRICT_EVENTS_PLUS_STATS"
    if tier in {"EVENTS_ONLY_RESEARCH", "EVENTS_ONLY"}:
        return "EVENTS_ONLY_RESEARCH"
    return tier or "UNKNOWN"


def normalized_text(value: Any) -> str:
    return str(value or "").strip().lower()


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
    return "|".join(normalized_text(part) for part in parts)


def minute_bucket(signal: dict[str, Any], interval: int | None = None) -> str:
    bucket_size = interval or config_int("FQIS_PAPER_ALERT_CANONICAL_MINUTE_BUCKET", DEFAULT_MINUTE_BUCKET)
    minute = fnum(signal.get("minute"))
    if minute is None:
        return ""
    bucket = int(minute // bucket_size) * bucket_size
    return str(bucket)


def canonical_alert_key(signal: dict[str, Any], interval: int | None = None) -> str:
    parts = [
        signal.get("fixture_id") or "",
        signal.get("market") or "",
        signal.get("selection") or "",
        signal.get("final_pipeline") or "",
        normalized_data_tier(signal),
        signal.get("research_bucket") or "",
        minute_bucket(signal, interval),
    ]
    return "|".join(normalized_text(part) for part in parts)


def load_state() -> dict[str, Any]:
    payload = read_json(STATE_JSON)
    if payload.get("missing") or payload.get("error"):
        return {"version": 2, "seen_alert_keys": {}, "seen_canonical_alert_keys": {}}
    seen = payload.get("seen_alert_keys")
    if isinstance(seen, list):
        seen = {str(key): {"first_seen_utc": "", "last_seen_utc": "", "count": 1} for key in seen}
    if not isinstance(seen, dict):
        seen = {}
    canonical_seen = payload.get("seen_canonical_alert_keys")
    if not isinstance(canonical_seen, dict):
        canonical_seen = {}
    return {"version": 2, "seen_alert_keys": seen, "seen_canonical_alert_keys": canonical_seen}


def initial_canonical_state(signal: dict[str, Any], alert_key: str, canonical_key: str, generated_at_utc: str) -> dict[str, Any]:
    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))
    return {
        "canonical_alert_key": canonical_key,
        "first_seen_utc": generated_at_utc,
        "last_seen_utc": generated_at_utc,
        "seen_count": 1,
        "fixture_id": signal.get("fixture_id"),
        "market": signal.get("market"),
        "selection": signal.get("selection"),
        "minute_bucket": minute_bucket(signal),
        "final_pipeline": signal.get("final_pipeline"),
        "data_tier": normalized_data_tier(signal),
        "research_bucket": signal.get("research_bucket"),
        "alert_key_latest": alert_key,
        "odds_first": odds,
        "odds_latest": odds,
        "odds_min": odds,
        "odds_max": odds,
        "ev_first": ev,
        "ev_latest": ev,
        "ev_max": ev,
        "edge_first": edge,
        "edge_latest": edge,
        "edge_max": edge,
    }


def material_update_reasons(signal: dict[str, Any], previous: dict[str, Any]) -> list[str]:
    odds_threshold = config_float("FQIS_PAPER_ALERT_ODDS_MATERIAL_THRESHOLD", DEFAULT_ODDS_MATERIAL_THRESHOLD)
    ev_threshold = config_float("FQIS_PAPER_ALERT_EV_MATERIAL_THRESHOLD", DEFAULT_EV_MATERIAL_THRESHOLD)
    edge_threshold = config_float("FQIS_PAPER_ALERT_EDGE_MATERIAL_THRESHOLD", DEFAULT_EDGE_MATERIAL_THRESHOLD)
    reasons: list[str] = []

    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    previous_odds = fnum(previous.get("odds_latest"))
    if odds is not None and previous_odds is not None and abs(odds - previous_odds) >= odds_threshold:
        reasons.append("MATERIAL_ODDS_CHANGE")

    ev = fnum(signal.get("ev_real"))
    previous_ev_max = fnum(previous.get("ev_max"))
    if ev is not None and previous_ev_max is not None and ev - previous_ev_max >= ev_threshold:
        reasons.append("MATERIAL_EV_INCREASE")

    edge = fnum(signal.get("edge_prob"))
    previous_edge_max = fnum(previous.get("edge_max"))
    if edge is not None and previous_edge_max is not None and edge - previous_edge_max >= edge_threshold:
        reasons.append("MATERIAL_EDGE_INCREASE")

    previous_pipeline = normalized_text(previous.get("final_pipeline"))
    current_pipeline = normalized_text(signal.get("final_pipeline"))
    if (
        previous_pipeline
        and current_pipeline
        and PIPELINE_PRIORITY.get(current_pipeline, 99) < PIPELINE_PRIORITY.get(previous_pipeline, 99)
    ):
        reasons.append("MATERIAL_PIPELINE_IMPROVED")

    previous_tier = str(previous.get("data_tier") or "UNKNOWN")
    current_tier = normalized_data_tier(signal)
    if DATA_TIER_PRIORITY.get(current_tier, 99) < DATA_TIER_PRIORITY.get(previous_tier, 99):
        reasons.append("MATERIAL_DATA_TIER_IMPROVED")

    return reasons


def update_canonical_state(
    signal: dict[str, Any],
    alert_key: str,
    previous: dict[str, Any],
    generated_at_utc: str,
) -> dict[str, Any]:
    updated = dict(previous)
    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))

    updated["last_seen_utc"] = generated_at_utc
    updated["seen_count"] = int(updated.get("seen_count") or updated.get("count") or 0) + 1
    updated["alert_key_latest"] = alert_key
    updated["final_pipeline"] = signal.get("final_pipeline")
    updated["data_tier"] = normalized_data_tier(signal)
    updated["research_bucket"] = signal.get("research_bucket")

    if updated.get("first_seen_utc") is None:
        updated["first_seen_utc"] = generated_at_utc
    if odds is not None:
        updated["odds_latest"] = odds
        existing_min = fnum(updated.get("odds_min"))
        existing_max = fnum(updated.get("odds_max"))
        updated["odds_min"] = odds if existing_min is None else min(existing_min, odds)
        updated["odds_max"] = odds if existing_max is None else max(existing_max, odds)
    if ev is not None:
        updated["ev_latest"] = ev
        existing_ev_max = fnum(updated.get("ev_max"))
        updated["ev_max"] = ev if existing_ev_max is None else max(existing_ev_max, ev)
    if edge is not None:
        updated["edge_latest"] = edge
        existing_edge_max = fnum(updated.get("edge_max"))
        updated["edge_max"] = edge if existing_edge_max is None else max(existing_edge_max, edge)
    return updated


def alert_record(
    signal: dict[str, Any],
    alert_key: str,
    canonical_key: str,
    canonical_state: dict[str, Any],
    *,
    repeated: bool,
    lifecycle_status: str,
    material_reasons: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "alert_key": alert_key,
        "canonical_alert_key": canonical_key,
        "repeated": repeated,
        "alert_lifecycle_status": lifecycle_status,
        "material_update_reasons": material_reasons or [],
        "first_seen_utc": canonical_state.get("first_seen_utc"),
        "last_seen_utc": canonical_state.get("last_seen_utc"),
        "seen_count": canonical_state.get("seen_count") or canonical_state.get("count") or 1,
        "odds_first": canonical_state.get("odds_first"),
        "odds_latest": canonical_state.get("odds_latest"),
        "odds_min": canonical_state.get("odds_min"),
        "odds_max": canonical_state.get("odds_max"),
        "ev_first": canonical_state.get("ev_first"),
        "ev_latest": canonical_state.get("ev_latest"),
        "ev_max": canonical_state.get("ev_max"),
        "edge_first": canonical_state.get("edge_first"),
        "edge_latest": canonical_state.get("edge_latest"),
        "edge_max": canonical_state.get("edge_max"),
        "fixture_id": signal.get("fixture_id"),
        "match": signal.get("match"),
        "minute": signal.get("minute"),
        "minute_bucket": canonical_state.get("minute_bucket") or minute_bucket(signal),
        "score": signal.get("score"),
        "market": signal.get("market"),
        "selection": signal.get("selection"),
        "odds": signal.get("odds"),
        "edge_prob": signal.get("edge_prob"),
        "ev_real": signal.get("ev_real"),
        "final_pipeline": signal.get("final_pipeline"),
        "data_tier": normalized_data_tier(signal),
        "research_bucket": signal.get("research_bucket"),
        "paper_action": signal.get("paper_action"),
        "discord_sendable": lifecycle_status in {"NEW_CANONICAL", "UPDATED_CANONICAL"},
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


def lifecycle_priority(lifecycle_status: str) -> int:
    return {
        "REPEATED_CANONICAL": 1,
        "UPDATED_CANONICAL": 2,
        "NEW_CANONICAL": 3,
    }.get(lifecycle_status, 0)


def select_canonical_cycle_record(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return candidate
    current_priority = lifecycle_priority(str(current.get("alert_lifecycle_status") or ""))
    candidate_priority = lifecycle_priority(str(candidate.get("alert_lifecycle_status") or ""))
    if candidate_priority > current_priority:
        return candidate
    if candidate_priority < current_priority:
        return current
    current_ev = fnum(current.get("ev_latest", current.get("ev_real")))
    candidate_ev = fnum(candidate.get("ev_latest", candidate.get("ev_real")))
    if candidate_ev is not None and (current_ev is None or candidate_ev > current_ev):
        return candidate
    current_edge = fnum(current.get("edge_latest", current.get("edge_prob")))
    candidate_edge = fnum(candidate.get("edge_latest", candidate.get("edge_prob")))
    if candidate_edge is not None and (current_edge is None or candidate_edge > current_edge):
        return candidate
    return current


def signal_value_tuple(signal: dict[str, Any]) -> tuple[float, float, float]:
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))
    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    return (
        ev if ev is not None else -999.0,
        edge if edge is not None else -999.0,
        odds if odds is not None else -999.0,
    )


def select_canonical_signal(
    current: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return candidate
    if signal_value_tuple(candidate["signal"]) > signal_value_tuple(current["signal"]):
        return candidate
    return current


def build_payload() -> tuple[dict[str, Any], dict[str, Any] | None]:
    generated_at_utc = utc_now()
    export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    state = load_state()
    seen: dict[str, Any] = state["seen_alert_keys"]
    canonical_seen: dict[str, Any] = state["seen_canonical_alert_keys"]
    state_before_size = len(seen)
    canonical_state_before_size = len(canonical_seen)

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
            "raw_new_alerts": 0,
            "raw_repeated_alerts": 0,
            "new_canonical_alerts": 0,
            "updated_canonical_alerts": 0,
            "repeated_canonical_alerts": 0,
            "suppressed_exact_repeats": 0,
            "material_updates": 0,
            "discord_sendable_canonical_only": False,
            "state_size": state_before_size,
            "canonical_state_size": canonical_state_before_size,
            "new_alert_records": [],
            "updated_alert_records": [],
            "repeated_alert_records": [],
            "raw_new_alert_records": [],
            "raw_repeated_alert_records": [],
            **SAFETY_BLOCK,
        }
        return payload, None

    canonical_cycle_records: dict[str, dict[str, Any]] = {}
    canonical_signal_by_key: dict[str, dict[str, Any]] = {}
    alertable_items: list[dict[str, Any]] = []
    raw_new_records: list[dict[str, Any]] = []
    raw_repeated_records: list[dict[str, Any]] = []
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        if not is_alertable_signal(signal):
            continue
        alert_key = stable_alert_key(signal)
        if not alert_key.strip("|"):
            continue
        canonical_key = canonical_alert_key(signal)
        if not canonical_key.strip("|"):
            continue

        exact_repeated = alert_key in seen
        if alert_key in seen:
            seen_record = seen.get(alert_key) if isinstance(seen.get(alert_key), dict) else {}
            seen_record["last_seen_utc"] = generated_at_utc
            seen_record["count"] = int(seen_record.get("count") or 1) + 1
            seen[alert_key] = seen_record
        else:
            seen[alert_key] = {
                "first_seen_utc": generated_at_utc,
                "last_seen_utc": generated_at_utc,
                "count": 1,
                "fixture_id": signal.get("fixture_id"),
                "selection": signal.get("selection"),
                "market": signal.get("market"),
                "final_pipeline": signal.get("final_pipeline"),
                "odds": signal.get("odds"),
                "canonical_alert_key": canonical_key,
            }

        item = {
            "signal": signal,
            "alert_key": alert_key,
            "canonical_key": canonical_key,
            "exact_repeated": exact_repeated,
        }
        alertable_items.append(item)
        canonical_signal_by_key[canonical_key] = select_canonical_signal(
            canonical_signal_by_key.get(canonical_key),
            item,
        )

    for item in canonical_signal_by_key.values():
        signal = item["signal"]
        alert_key = item["alert_key"]
        canonical_key = item["canonical_key"]
        previous_canonical = canonical_seen.get(canonical_key)
        if isinstance(previous_canonical, dict):
            reasons_for_update = material_update_reasons(signal, previous_canonical)
            next_canonical = update_canonical_state(signal, alert_key, previous_canonical, generated_at_utc)
            canonical_seen[canonical_key] = next_canonical
            lifecycle = "UPDATED_CANONICAL" if reasons_for_update else "REPEATED_CANONICAL"
            record = alert_record(
                signal,
                alert_key,
                canonical_key,
                next_canonical,
                repeated=True,
                lifecycle_status=lifecycle,
                material_reasons=reasons_for_update,
            )
        else:
            next_canonical = initial_canonical_state(signal, alert_key, canonical_key, generated_at_utc)
            canonical_seen[canonical_key] = next_canonical
            record = alert_record(
                signal,
                alert_key,
                canonical_key,
                next_canonical,
                repeated=False,
                lifecycle_status="NEW_CANONICAL",
            )

        canonical_cycle_records[canonical_key] = select_canonical_cycle_record(
            canonical_cycle_records.get(canonical_key),
            record,
        )

    for item in alertable_items:
        signal = item["signal"]
        alert_key = item["alert_key"]
        canonical_key = item["canonical_key"]
        canonical_record = canonical_cycle_records.get(canonical_key)
        canonical_state = canonical_seen.get(canonical_key) if isinstance(canonical_seen.get(canonical_key), dict) else {}
        if not canonical_record:
            continue
        raw_record = alert_record(
            signal,
            alert_key,
            canonical_key,
            canonical_state,
            repeated=canonical_record.get("alert_lifecycle_status") != "NEW_CANONICAL",
            lifecycle_status=str(canonical_record.get("alert_lifecycle_status") or "REPEATED_CANONICAL"),
            material_reasons=canonical_record.get("material_update_reasons") or [],
        )
        if item["exact_repeated"]:
            raw_repeated_records.append(raw_record)
        else:
            raw_new_records.append(raw_record)

    next_state = {
        "version": 2,
        "updated_at_utc": generated_at_utc,
        "seen_alert_keys": seen,
        "seen_canonical_alert_keys": canonical_seen,
    }
    canonical_records = list(canonical_cycle_records.values())
    new_records = [
        record for record in canonical_records if record.get("alert_lifecycle_status") == "NEW_CANONICAL"
    ]
    updated_records = [
        record for record in canonical_records if record.get("alert_lifecycle_status") == "UPDATED_CANONICAL"
    ]
    repeated_records = [
        record for record in canonical_records if record.get("alert_lifecycle_status") == "REPEATED_CANONICAL"
    ]
    material_update_count = len(updated_records)
    suppressed_exact_repeats = len(raw_repeated_records) + len(
        [
            record
            for record in raw_new_records
            if record.get("alert_lifecycle_status") == "REPEATED_CANONICAL"
        ]
    )
    payload = {
        "mode": "FQIS_PAPER_ALERT_DEDUPE",
        "status": "READY",
        "generated_at_utc": generated_at_utc,
        "reasons": [],
        "safety": dict(SAFETY_BLOCK),
        "dedupe_config": {
            "minute_bucket_interval": config_int(
                "FQIS_PAPER_ALERT_CANONICAL_MINUTE_BUCKET",
                DEFAULT_MINUTE_BUCKET,
            ),
            "odds_material_threshold": config_float(
                "FQIS_PAPER_ALERT_ODDS_MATERIAL_THRESHOLD",
                DEFAULT_ODDS_MATERIAL_THRESHOLD,
            ),
            "ev_material_threshold": config_float(
                "FQIS_PAPER_ALERT_EV_MATERIAL_THRESHOLD",
                DEFAULT_EV_MATERIAL_THRESHOLD,
            ),
            "edge_material_threshold": config_float(
                "FQIS_PAPER_ALERT_EDGE_MATERIAL_THRESHOLD",
                DEFAULT_EDGE_MATERIAL_THRESHOLD,
            ),
        },
        "total_signals": len(signals),
        "new_alerts": len(new_records),
        "repeated_alerts": len(repeated_records),
        "suppressed_repeats": suppressed_exact_repeats,
        "raw_new_alerts": len(raw_new_records),
        "raw_repeated_alerts": len(raw_repeated_records),
        "new_canonical_alerts": len(new_records),
        "updated_canonical_alerts": len(updated_records),
        "repeated_canonical_alerts": len(repeated_records),
        "suppressed_exact_repeats": suppressed_exact_repeats,
        "material_updates": material_update_count,
        "discord_sendable_canonical_only": bool(new_records or updated_records),
        "state_size": len(seen),
        "canonical_state_size": len(canonical_seen),
        "state_size_before": state_before_size,
        "canonical_state_size_before": canonical_state_before_size,
        "state_file": str(STATE_JSON),
        "new_alert_records": new_records,
        "updated_alert_records": updated_records,
        "repeated_alert_records": repeated_records,
        "raw_new_alert_records": raw_new_records,
        "raw_repeated_alert_records": raw_repeated_records,
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
        f"- raw_new_alerts: **{payload.get('raw_new_alerts', 0)}**",
        f"- new_canonical_alerts: **{payload.get('new_canonical_alerts', 0)}**",
        f"- updated_canonical_alerts: **{payload.get('updated_canonical_alerts', 0)}**",
        f"- repeated_canonical_alerts: **{payload.get('repeated_canonical_alerts', 0)}**",
        f"- suppressed_exact_repeats: **{payload.get('suppressed_exact_repeats', 0)}**",
        f"- material_updates: **{payload.get('material_updates', 0)}**",
        f"- state_size: **{payload.get('state_size', 0)}**",
        f"- canonical_state_size: **{payload.get('canonical_state_size', 0)}**",
        "- can_execute_real_bets: **False**",
        "- can_enable_live_staking: **False**",
        "- can_mutate_ledger: **False**",
        "- live_staking_allowed: **False**",
        "- promotion_allowed: **False**",
        "",
        "## Canonical Sendable Paper Alerts",
        "",
    ]
    records = [*(payload.get("new_alert_records") or []), *(payload.get("updated_alert_records") or [])]
    if not records:
        lines.append("- None.")
    else:
        for record in records[:25]:
            lines.append(
                "- {status} | {match} | {minute}' | {selection} | odds {odds_latest} | EV {ev_latest} | edge {edge_latest} | {paper_action}".format(
                    status=safe_text(record.get("alert_lifecycle_status")),
                    match=safe_text(record.get("match") or record.get("fixture_id")),
                    minute=safe_text(record.get("minute")),
                    selection=safe_text(record.get("selection")),
                    odds_latest=safe_text(record.get("odds_latest")),
                    ev_latest=safe_text(record.get("ev_latest")),
                    edge_latest=safe_text(record.get("edge_latest")),
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
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())


