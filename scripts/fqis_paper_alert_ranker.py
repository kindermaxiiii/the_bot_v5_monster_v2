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
OUT_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
OUT_MD = ORCH_DIR / "latest_paper_alert_ranker.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "paper_only": True,
}

PAPER_ACTION_PRIORITY = {
    "PAPER_PRODUCTION_SIM_ONLY": 0,
    "PAPER_RESEARCH_WATCH": 1,
    "PAPER_REJECTED_NO_ACTION": 9,
}

BUCKET_ACTION_PRIORITY = {
    "KEEP_RESEARCH_BUCKET": 0,
    "WATCHLIST_BUCKET": 1,
    "INSUFFICIENT_SAMPLE": 2,
    "KILL_OR_QUARANTINE_BUCKET": 3,
}

DATA_TIER_PRIORITY = {
    "STRICT_EVENTS_PLUS_STATS": 0,
    "EVENTS_ONLY_RESEARCH": 1,
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


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


def fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return safe_text(value)


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


def normalized_data_tier(signal: dict[str, Any]) -> str:
    tier = str(signal.get("research_data_tier") or signal.get("data_tier") or "").strip()
    if tier in {"STRICT_EVENTS_PLUS_STATS", "EVENTS_PLUS_STATS"}:
        return "STRICT_EVENTS_PLUS_STATS"
    if tier in {"EVENTS_ONLY_RESEARCH", "EVENTS_ONLY"}:
        return "EVENTS_ONLY_RESEARCH"
    return tier or "UNKNOWN"


def odds_sanity(signal: dict[str, Any]) -> str:
    odds = fnum(signal.get("odds") or signal.get("entry_odds"))
    if odds is None:
        return "MISSING_ODDS_REVIEW"
    if odds <= 1.0 or odds > 100.0:
        return "ODDS_OUT_OF_RANGE_REVIEW"
    return "OK"


def is_alertable(signal: dict[str, Any]) -> bool:
    if signal.get("paper_action") == "PAPER_REJECTED_NO_ACTION":
        return False
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))
    if ev is not None and ev <= 0:
        return False
    if edge is not None and edge <= 0:
        return False
    return True


def dedupe_key_map(records: Any, status: str) -> dict[str, dict[str, Any]]:
    if not isinstance(records, list):
        return {}
    mapped: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        key = str(record.get("alert_key") or stable_alert_key(record))
        if not key.strip("|"):
            continue
        mapped[key] = {
            "dedupe_status": status,
            "repeated": bool(record.get("repeated")),
            "alert_key": key,
        }
    return mapped


def build_reasons(signal: dict[str, Any], dedupe_status: str, data_tier: str, odds_check: str) -> list[str]:
    reasons = [
        f"dedupe_status={dedupe_status}",
        f"paper_action={signal.get('paper_action')}",
        f"bucket_policy_action={signal.get('bucket_policy_action')}",
        f"data_tier={data_tier}",
        f"odds_sanity={odds_check}",
    ]
    final_reason = signal.get("final_pipeline_reason")
    if final_reason:
        reasons.append(f"pipeline_reason={final_reason}")
    return [safe_text(reason) for reason in reasons if safe_text(reason)]


def build_red_flags(signal: dict[str, Any], data_tier: str, odds_check: str) -> list[str]:
    red_flags: list[str] = []
    if odds_check != "OK":
        red_flags.append(odds_check)
    if signal.get("bucket_policy_action") == "KILL_OR_QUARANTINE_BUCKET":
        red_flags.append("KILL_OR_QUARANTINE_BUCKET")
    if data_tier == "UNKNOWN":
        red_flags.append("UNKNOWN_DATA_TIER")
    ev = fnum(signal.get("ev_real"))
    edge = fnum(signal.get("edge_prob"))
    if ev is None:
        red_flags.append("MISSING_EV")
    if edge is None:
        red_flags.append("MISSING_EDGE")
    return red_flags


def operator_note(dedupe_status: str, red_flags: list[str]) -> str:
    if red_flags:
        flags = ", ".join(red_flags)
        return f"PAPER ONLY - review red flags ({flags}). NO REAL BET / NO STAKE / NO EXECUTION."
    if dedupe_status == "NEW":
        return "PAPER ONLY - new ranked alert for observation. NO REAL BET / NO STAKE / NO EXECUTION."
    if dedupe_status == "REPEATED":
        return "PAPER ONLY - repeated ranked alert, observe only. NO REAL BET / NO STAKE / NO EXECUTION."
    return "PAPER ONLY - ranked watch item, observe only. NO REAL BET / NO STAKE / NO EXECUTION."


def normalize_alert(signal: dict[str, Any], dedupe_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    alert_key = stable_alert_key(signal)
    dedupe = dedupe_lookup.get(alert_key) or {}
    dedupe_status = str(dedupe.get("dedupe_status") or "CURRENT_UNTRACKED")
    data_tier = normalized_data_tier(signal)
    odds_check = odds_sanity(signal)
    red_flags = build_red_flags(signal, data_tier, odds_check)

    return {
        "rank": None,
        "alert_key": alert_key,
        "dedupe_status": dedupe_status,
        "is_new_alert": dedupe_status == "NEW",
        "is_repeated_alert": dedupe_status == "REPEATED",
        "fixture_id": signal.get("fixture_id"),
        "match": signal.get("match"),
        "league": signal.get("league"),
        "minute": signal.get("minute"),
        "score": signal.get("score"),
        "market": signal.get("market"),
        "selection": signal.get("selection"),
        "odds": signal.get("odds"),
        "p_model": signal.get("p_model"),
        "implied_probability": signal.get("implied_probability"),
        "edge_prob": signal.get("edge_prob"),
        "ev_real": signal.get("ev_real"),
        "paper_action": signal.get("paper_action"),
        "final_pipeline": signal.get("final_pipeline"),
        "research_bucket": signal.get("research_bucket"),
        "bucket_policy_action": signal.get("bucket_policy_action"),
        "data_tier": data_tier,
        "odds_sanity": odds_check,
        "reasons": build_reasons(signal, dedupe_status, data_tier, odds_check),
        "red_flags": red_flags,
        "operator_note": operator_note(dedupe_status, red_flags),
        "paper_only": True,
        **SAFETY_BLOCK,
    }


def sort_key(alert: dict[str, Any]) -> tuple[int, int, float, float, int, int]:
    action_priority = PAPER_ACTION_PRIORITY.get(str(alert.get("paper_action")), 99)
    bucket_priority = BUCKET_ACTION_PRIORITY.get(str(alert.get("bucket_policy_action")), 99)
    ev = fnum(alert.get("ev_real"))
    edge = fnum(alert.get("edge_prob"))
    odds_priority = 0 if alert.get("odds_sanity") == "OK" else 1
    tier_priority = DATA_TIER_PRIORITY.get(str(alert.get("data_tier")), 99)
    return (
        action_priority,
        bucket_priority,
        -(ev if ev is not None else -999.0),
        -(edge if edge is not None else -999.0),
        odds_priority,
        tier_priority,
    )


def rank_alerts(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(alerts, key=sort_key)
    primary: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    seen_fixture_selection: set[str] = set()

    for alert in ordered:
        duplicate_key = "|".join(
            safe_text(alert.get(name)).lower()
            for name in ("fixture_id", "market", "selection")
        )
        if duplicate_key.strip("|") and duplicate_key not in seen_fixture_selection:
            primary.append(alert)
            seen_fixture_selection.add(duplicate_key)
        else:
            deferred.append(alert)

    ranked = [*primary, *deferred]
    for index, alert in enumerate(ranked, start=1):
        alert["rank"] = index
    return ranked


def rejected_summary(signals: list[dict[str, Any]]) -> dict[str, Any]:
    rejected = [signal for signal in signals if signal.get("paper_action") == "PAPER_REJECTED_NO_ACTION"]
    reasons: dict[str, int] = {}
    for signal in rejected:
        reason = safe_text(signal.get("rejection_reason") or signal.get("final_pipeline_reason") or "UNKNOWN")
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "rejected_count": len(rejected),
        "top_rejection_reasons": [
            {"reason": reason, "count": count}
            for reason, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


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

    raw_signals = export.get("signals") or []
    if not isinstance(raw_signals, list):
        raw_signals = []
        reasons.append("PAPER_SIGNALS_NOT_LIST")
    signals = [signal for signal in raw_signals if isinstance(signal, dict)]

    new_lookup = dedupe_key_map(dedupe.get("new_alert_records"), "NEW")
    repeated_lookup = dedupe_key_map(dedupe.get("repeated_alert_records"), "REPEATED")
    dedupe_lookup = {**repeated_lookup, **new_lookup}

    alertable = [normalize_alert(signal, dedupe_lookup) for signal in signals if is_alertable(signal)]
    ranked_alerts = rank_alerts(alertable)
    top_ranked_alerts = ranked_alerts[:25]

    status = "BLOCKED" if reasons else "READY"
    return {
        "mode": "FQIS_PAPER_ALERT_RANKER",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "reasons": reasons,
        "safety": dict(SAFETY_BLOCK),
        "source_files": {
            "paper_signal_export": str(PAPER_SIGNAL_EXPORT_JSON),
            "paper_alert_dedupe": str(PAPER_ALERT_DEDUPE_JSON),
            "go_no_go": str(GO_NO_GO_JSON),
            "shadow_readiness": str(SHADOW_READINESS_JSON),
            "live_freshness": str(LIVE_FRESHNESS_JSON),
        },
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "live_freshness_status": freshness.get("status"),
        "live_freshness_flags": freshness.get("freshness_flags") or [],
        "historical_metric_static_review": freshness.get("historical_metric_static_review") or [],
        "paper_signals_total": len(signals),
        "ranked_alert_count": len(ranked_alerts),
        "top_ranked_alert_count": len(top_ranked_alerts),
        "new_ranked_alert_count": sum(1 for alert in ranked_alerts if alert.get("is_new_alert")),
        "repeated_ranked_alert_count": sum(1 for alert in ranked_alerts if alert.get("is_repeated_alert")),
        "dedupe_new_alerts": dedupe.get("new_alerts") or 0,
        "dedupe_repeated_alerts": dedupe.get("repeated_alerts") or 0,
        "rejected_blocked_summary": rejected_summary(signals),
        "top_ranked_alerts": top_ranked_alerts,
        "ranked_alerts": ranked_alerts,
        "unsafe_flag_paths": unsafe_hits,
        **SAFETY_BLOCK,
    }


def write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# FQIS Paper Alert Ranker",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        "",
        f"- status: **{payload.get('status')}**",
        f"- ranked_alert_count: **{payload.get('ranked_alert_count', 0)}**",
        f"- new_ranked_alert_count: **{payload.get('new_ranked_alert_count', 0)}**",
        f"- repeated_ranked_alert_count: **{payload.get('repeated_ranked_alert_count', 0)}**",
        f"- can_execute_real_bets: **{payload.get('can_execute_real_bets')}**",
        f"- can_enable_live_staking: **{payload.get('can_enable_live_staking')}**",
        f"- can_mutate_ledger: **{payload.get('can_mutate_ledger')}**",
        "",
        "## Top Ranked Paper Alerts",
        "",
        "| Rank | Match | Minute | Score | Selection | Odds | Edge | EV | Tier | Bucket action | Paper action | Note |",
        "|---:|---|---:|---|---|---:|---:|---:|---|---|---|---|",
    ]

    alerts = payload.get("top_ranked_alerts") or []
    if not alerts:
        lines.append("|  | No ranked paper alerts |  |  |  |  |  |  |  |  |  | PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION |")
    else:
        for alert in alerts:
            lines.append(
                "| {rank} | {match} | {minute} | {score} | {selection} | {odds} | {edge} | {ev} | {tier} | {bucket} | {action} | {note} |".format(
                    rank=fmt(alert.get("rank")),
                    match=fmt(alert.get("match") or alert.get("fixture_id")),
                    minute=fmt(alert.get("minute")),
                    score=fmt(alert.get("score")),
                    selection=fmt(alert.get("selection")),
                    odds=fmt(alert.get("odds")),
                    edge=fmt(alert.get("edge_prob")),
                    ev=fmt(alert.get("ev_real")),
                    tier=fmt(alert.get("data_tier")),
                    bucket=fmt(alert.get("bucket_policy_action")),
                    action=fmt(alert.get("paper_action")),
                    note=fmt(alert.get("operator_note")),
                )
            )

    rejected = payload.get("rejected_blocked_summary") or {}
    lines += [
        "",
        "## Rejected / Blocked Summary",
        "",
        f"- rejected_count: **{rejected.get('rejected_count', 0)}**",
    ]
    for item in rejected.get("top_rejection_reasons") or []:
        lines.append(f"- {item.get('count')}: {safe_text(item.get('reason'))}")

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
