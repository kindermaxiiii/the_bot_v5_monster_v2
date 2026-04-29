from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DECISION_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

LIVE_DECISIONS_JSON = DECISION_DIR / "latest_live_decisions.json"
FINAL_PIPELINE_AUDIT_JSON = DECISION_DIR / "latest_final_pipeline_audit.json"
BUCKET_POLICY_AUDIT_JSON = RESEARCH_DIR / "latest_bucket_policy_audit.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_READINESS_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
OUT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
OUT_MD = ORCH_DIR / "latest_paper_signal_export.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
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


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
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


def research_data_tier(decision: dict[str, Any]) -> str:
    payload = decision.get("payload") or {}
    mode = str(payload.get("level3_data_mode") or "")
    if payload.get("level3_trade_ready") is True and mode == "EVENTS_PLUS_STATS":
        return "STRICT_EVENTS_PLUS_STATS"
    if payload.get("level3_state_ready") is True and mode == "EVENTS_ONLY":
        return "EVENTS_ONLY_RESEARCH"
    return "REJECTED_DATA_TIER"


def research_bucket(decision: dict[str, Any]) -> str:
    data_tier = research_data_tier(decision)
    vetoes = [str(v) for v in decision.get("vetoes") or []]
    selection = str(decision.get("selection") or "").upper()
    prefix = "EVENTS_ONLY" if data_tier == "EVENTS_ONLY_RESEARCH" else "STRICT"

    if "UNDER 0.5" in selection or any("under_0_5" in v for v in vetoes):
        return f"{prefix}_UNDER_0_5_RESEARCH"
    if "UNDER 1.5" in selection or any("under_1_5" in v for v in vetoes):
        return f"{prefix}_UNDER_1_5_RESEARCH"
    if "UNDER 2.5" in selection or any("under_2_5" in v for v in vetoes):
        return f"{prefix}_UNDER_2_5_RESEARCH"
    if "UNDER" in selection:
        return f"{prefix}_UNDER_GENERAL_RESEARCH"
    if "OVER" in selection:
        return f"{prefix}_OVER_RESEARCH"
    return f"{prefix}_MARKET_RESEARCH"


def paper_action(final_pipeline: str) -> str:
    pipeline = final_pipeline.lower()
    if pipeline == "production":
        return "PAPER_PRODUCTION_SIM_ONLY"
    if pipeline == "research":
        return "PAPER_RESEARCH_WATCH"
    return "PAPER_REJECTED_NO_ACTION"


def rejection_reason(decision: dict[str, Any], final_pipeline: str, final_pipeline_reason: str) -> str:
    if final_pipeline.lower() != "reject":
        return ""
    payload = decision.get("payload") or {}
    reasons = [
        final_pipeline_reason,
        payload.get("primary_veto"),
        ", ".join(str(v) for v in decision.get("vetoes") or []),
    ]
    return "; ".join(str(reason) for reason in reasons if reason)


def normalize_signal(decision: dict[str, Any], bucket_policies: dict[str, Any]) -> dict[str, Any]:
    payload = decision.get("payload") or {}
    final_pipeline = str(
        first_present(payload.get("final_pipeline"), payload.get("level3_pipeline"), "reject")
    ).lower()
    final_pipeline_reason = str(
        first_present(payload.get("final_pipeline_reason"), payload.get("level3_route_reason"), "")
    )
    bucket = research_bucket(decision)
    bucket_policy = bucket_policies.get(bucket) or {}
    odds = first_present(decision.get("odds_decimal"), payload.get("odds_decimal"), decision.get("entry_odds"))
    p_model = first_present(decision.get("calibrated_probability"), decision.get("raw_probability"))
    implied_probability = first_present(decision.get("market_no_vig_probability"), payload.get("market_no_vig_probability"))
    if implied_probability is None:
        odds_float = fnum(odds)
        implied_probability = round(1 / odds_float, 6) if odds_float else None

    return {
        "paper_only": True,
        "decision_key": decision.get("decision_key"),
        "fixture_id": first_present(decision.get("fixture_id"), payload.get("fixture_id")),
        "match": first_present(decision.get("match"), payload.get("match")),
        "league": first_present(decision.get("league"), payload.get("league")),
        "minute": first_present(decision.get("minute"), payload.get("minute")),
        "score": decision.get("score"),
        "market": first_present(decision.get("market"), decision.get("market_key"), payload.get("raw_market_name")),
        "selection": decision.get("selection"),
        "odds": odds,
        "entry_odds": odds,
        "p_model": p_model,
        "implied_probability": implied_probability,
        "edge_prob": decision.get("edge"),
        "ev_real": decision.get("expected_value"),
        "final_pipeline": final_pipeline,
        "final_pipeline_reason": final_pipeline_reason,
        "level3_gate_state": payload.get("level3_gate_state"),
        "research_bucket": bucket,
        "bucket_policy_action": bucket_policy.get("action"),
        "status": decision.get("publication_status") or decision.get("real_status") or "PAPER_ONLY",
        "paper_action": paper_action(final_pipeline),
        "rejection_reason": rejection_reason(decision, final_pipeline, final_pipeline_reason),
        "data_tier": payload.get("level3_data_mode"),
        "research_data_tier": research_data_tier(decision),
        "generated_at_utc": decision.get("generated_at_utc"),
        "raw_safety_flags": {
            "source_executable": decision.get("executable"),
            "source_real_status": decision.get("real_status"),
            "source_publication_status": decision.get("publication_status"),
            "decision_live_staking_allowed": payload.get("live_staking_allowed"),
            "level3_live_staking_allowed": payload.get("level3_live_staking_allowed"),
            "paper_only": True,
            **SAFETY_BLOCK,
        },
    }


def top_watchlist(signals: list[dict[str, Any]], limit: int = 25) -> list[dict[str, Any]]:
    def sort_key(signal: dict[str, Any]) -> tuple[float, float]:
        ev = fnum(signal.get("ev_real"))
        edge = fnum(signal.get("edge_prob"))
        return (ev if ev is not None else -999.0, edge if edge is not None else -999.0)

    def watchable(signal: dict[str, Any]) -> bool:
        if signal.get("paper_action") == "PAPER_REJECTED_NO_ACTION":
            return False
        ev = fnum(signal.get("ev_real"))
        edge = fnum(signal.get("edge_prob"))
        if ev is not None and ev <= 0:
            return False
        if edge is not None and edge <= 0:
            return False
        return True

    candidates = [signal for signal in signals if watchable(signal)]
    return sorted(candidates or signals, key=sort_key, reverse=True)[:limit]


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    live_decisions = read_json(LIVE_DECISIONS_JSON)
    final_pipeline_audit = read_json(FINAL_PIPELINE_AUDIT_JSON)
    bucket_policy_audit = read_json(BUCKET_POLICY_AUDIT_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_READINESS_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)

    inputs = {
        "live_decisions": live_decisions,
        "final_pipeline_audit": final_pipeline_audit,
        "bucket_policy_audit": bucket_policy_audit,
        "go_no_go": go_no_go,
        "shadow_readiness": shadow,
        "live_freshness": freshness,
    }

    reasons: list[str] = []
    missing_inputs = [
        name for name, payload in inputs.items() if payload.get("missing") or payload.get("error")
    ]
    if missing_inputs:
        reasons.append("MISSING_INPUTS:" + ",".join(missing_inputs))

    if go_no_go.get("status") != "READY":
        reasons.append("GO_NO_GO_NOT_READY")
    if shadow.get("status") != "READY" or shadow.get("shadow_state") != "SHADOW_READY":
        reasons.append("SHADOW_READINESS_NOT_READY")
    if freshness.get("status") == "MISSING_INPUTS":
        reasons.append("LIVE_FRESHNESS_MISSING_INPUTS")
    if final_pipeline_audit.get("live_staking_allowed_true_count", 0):
        reasons.append("FINAL_PIPELINE_LIVE_STAKING_TRUE_COUNT")

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

    decisions = live_decisions.get("decisions") or []
    if not isinstance(decisions, list):
        decisions = []
        reasons.append("LIVE_DECISIONS_NOT_LIST")

    bucket_policies = bucket_policy_audit.get("buckets") or {}
    signals = [
        normalize_signal(decision, bucket_policies)
        for decision in decisions
        if isinstance(decision, dict)
    ]

    counts = {
        "paper_production_sim_only_count": sum(
            1 for signal in signals if signal.get("paper_action") == "PAPER_PRODUCTION_SIM_ONLY"
        ),
        "paper_research_watch_count": sum(
            1 for signal in signals if signal.get("paper_action") == "PAPER_RESEARCH_WATCH"
        ),
        "paper_rejected_count": sum(
            1 for signal in signals if signal.get("paper_action") == "PAPER_REJECTED_NO_ACTION"
        ),
    }

    status = "BLOCKED" if reasons else "READY"
    return {
        "mode": "FQIS_PAPER_SIGNAL_EXPORT",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "reasons": reasons,
        "safety": dict(SAFETY_BLOCK),
        "source_files": {
            "live_decisions": str(LIVE_DECISIONS_JSON),
            "final_pipeline_audit": str(FINAL_PIPELINE_AUDIT_JSON),
            "bucket_policy_audit": str(BUCKET_POLICY_AUDIT_JSON),
            "go_no_go": str(GO_NO_GO_JSON),
            "shadow_readiness": str(SHADOW_READINESS_JSON),
            "live_freshness": str(LIVE_FRESHNESS_JSON),
        },
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "live_freshness_status": freshness.get("status"),
        "total_decisions": len(signals),
        "paper_signals_total": len(signals),
        **counts,
        "signals": signals,
        "top_paper_watchlist": top_watchlist(signals),
    }


def fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return safe_text(value)


def write_markdown(payload: dict[str, Any]) -> None:
    lines = [
        "# FQIS Paper Signal Export",
        "",
        f"- status: **{payload.get('status')}**",
        f"- generated_at_utc: `{payload.get('generated_at_utc')}`",
        f"- total decisions: **{payload.get('total_decisions', 0)}**",
        f"- paper production sim-only count: **{payload.get('paper_production_sim_only_count', 0)}**",
        f"- paper research watch count: **{payload.get('paper_research_watch_count', 0)}**",
        f"- paper rejected count: **{payload.get('paper_rejected_count', 0)}**",
        "- live staking allowed false",
        "- can execute real bets false",
        "",
        "## Top Paper Watchlist",
        "",
        "| Fixture | Minute | Score | Selection | Odds | Edge | EV | Pipeline | Bucket Action | Paper Action |",
        "|---|---:|---|---|---:|---:|---:|---|---|---|",
    ]

    for signal in payload.get("top_paper_watchlist") or []:
        lines.append(
            "| {fixture} | {minute} | {score} | {selection} | {odds} | {edge} | {ev} | {pipeline} | {bucket_action} | {paper_action} |".format(
                fixture=fmt(signal.get("match") or signal.get("fixture_id")),
                minute=fmt(signal.get("minute")),
                score=fmt(signal.get("score")),
                selection=fmt(signal.get("selection")),
                odds=fmt(signal.get("odds")),
                edge=fmt(signal.get("edge_prob")),
                ev=fmt(signal.get("ev_real")),
                pipeline=fmt(signal.get("final_pipeline")),
                bucket_action=fmt(signal.get("bucket_policy_action")),
                paper_action=fmt(signal.get("paper_action")),
            )
        )

    if payload.get("reasons"):
        lines += [
            "",
            "## Block Reasons",
            "",
            *[f"- {safe_text(reason)}" for reason in payload.get("reasons") or []],
        ]

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
