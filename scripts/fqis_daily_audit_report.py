from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

BRIDGE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
LEVEL3_JSON = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_level3_live_state.json"
RESEARCH_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_candidates.json"
SETTLEMENT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json"
PERFORMANCE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.json"
CLV_HORIZON_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.json"
PROVIDER_JSON = ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage" / "latest_provider_coverage_report.json"

OUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "audit"
OUT_MD = OUT_DIR / "latest_daily_audit_report.md"
OUT_JSON = OUT_DIR / "latest_daily_audit_report.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def fint(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def pct(value: Any) -> str:
    return f"{fnum(value) * 100:.2f}%"


def rate(n: int, d: int) -> float:
    return round(n / d, 6) if d else 0.0


def summary(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("summary") or {}


def research_diagnostics(research_payload: dict[str, Any]) -> dict[str, Any]:
    s = summary(research_payload)

    decisions_screened = fint(s.get("decisions_screened"))
    candidates = fint(s.get("candidates_this_cycle"))
    timing = fint(s.get("timing_policy_rejected"))
    data_tier = fint(s.get("data_tier_rejected"))
    edge_ev = fint(s.get("non_positive_edge_or_ev_rejected"))
    final_status = fint(s.get("final_status_rejected"))
    negative_value = fint(s.get("negative_value_veto_rejected"))

    return {
        "decisions_screened": decisions_screened,
        "candidates_this_cycle": candidates,
        "strict_events_plus_stats": fint(s.get("strict_events_plus_stats")),
        "events_only_research": fint(s.get("events_only_research")),
        "new_snapshots_appended": fint(s.get("new_snapshots_appended")),
        "existing_snapshots_before_append": fint(s.get("existing_snapshots_before_append")),
        "timing_policy_rejected": timing,
        "data_tier_rejected": data_tier,
        "non_positive_edge_or_ev_rejected": edge_ev,
        "final_status_rejected": final_status,
        "negative_value_veto_rejected": negative_value,
        "research_acceptance_rate": rate(candidates, decisions_screened),
        "timing_rejection_rate": rate(timing, decisions_screened),
        "data_tier_rejection_rate": rate(data_tier, decisions_screened),
        "non_positive_edge_or_ev_rejection_rate": rate(edge_ev, decisions_screened),
    }



def research_side_bias_metrics() -> dict[str, Any]:
    import csv
    from collections import Counter
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    ledger = root / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"

    if not ledger.exists():
        return {
            "research_rows_total": 0,
            "under_rows": 0,
            "over_rows": 0,
            "under_share": 0.0,
            "over_share": 0.0,
            "dominant_side": "",
        }

    rows = []
    try:
        with ledger.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        rows = []

    counter = Counter(str(r.get("side") or "").upper() for r in rows)
    total = len(rows)
    under = counter.get("UNDER", 0)
    over = counter.get("OVER", 0)

    under_share = under / total if total else 0.0
    over_share = over / total if total else 0.0

    if under_share > over_share:
        dominant = "UNDER"
    elif over_share > under_share:
        dominant = "OVER"
    else:
        dominant = "BALANCED"

    return {
        "research_rows_total": total,
        "under_rows": under,
        "over_rows": over,
        "under_share": round(under_share, 6),
        "over_share": round(over_share, 6),
        "dominant_side": dominant,
    }


def build_verdict(
    bridge_s: dict[str, Any],
    level3_s: dict[str, Any],
    provider_s: dict[str, Any],
    perf_s: dict[str, Any],
    clv_s: dict[str, Any],
    diag: dict[str, Any],
) -> dict[str, Any]:
    flags: list[str] = []

    official = fint(bridge_s.get("official_decisions"))
    watchlist = fint(bridge_s.get("watchlist_decisions"))

    current_state_ready = fint(level3_s.get("state_ready") or level3_s.get("ready_states"))
    current_trade_ready = fint(level3_s.get("trade_ready"))
    current_stats = fint(level3_s.get("stats_available"))
    current_events = fint(level3_s.get("events_available"))

    hist_stats_rate = fnum(provider_s.get("statistics_coverage_rate"))
    hist_events_stats_rate = fnum(provider_s.get("events_plus_stats_rate"))

    signal = perf_s.get("signal") or {}
    signal_settled = fint(signal.get("settled"))
    signal_roi = fnum(signal.get("roi_unit"))

    horizons = clv_s.get("horizons") or {}
    clv_5m = horizons.get("5m") or {}
    clv_5m_tracked = fint(clv_5m.get("tracked"))
    clv_5m_avg = fnum(clv_5m.get("avg"))

    if official == 0 and watchlist == 0:
        flags.append("NO_PUBLISHABLE_DECISION")

    if current_state_ready == 0:
        flags.append("NO_CURRENT_LEVEL3_STATE_READY")

    if current_trade_ready == 0:
        flags.append("NO_CURRENT_LEVEL3_TRADE_READY_FIXTURE")

    if current_events == 0:
        flags.append("CURRENT_LEVEL3_EVENTS_EMPTY")

    if current_stats == 0:
        flags.append("CURRENT_LEVEL3_STATS_EMPTY")

    if hist_stats_rate < 0.25:
        flags.append("LOW_HISTORICAL_LIVE_STATISTICS_COVERAGE")

    if hist_events_stats_rate < 0.15:
        flags.append("LOW_HISTORICAL_EVENTS_PLUS_STATS_COVERAGE")

    if signal_settled < 100:
        flags.append("INSUFFICIENT_RESEARCH_SAMPLE")

    if clv_5m_tracked < 100:
        flags.append("INSUFFICIENT_FIXED_HORIZON_CLV_SAMPLE")

    if diag["decisions_screened"] >= 30 and diag["research_acceptance_rate"] > 0.35:
        flags.append("RESEARCH_ACCEPTANCE_RATE_TOO_HIGH_REVIEW_FILTERS")

    promotion_allowed = False

    if not flags:
        final_verdict = "REVIEW_REQUIRED_NO_AUTOMATIC_PROMOTION"
    else:
        final_verdict = "NO_PROMOTION_KEEP_RESEARCH"

    return {
        "final_verdict": final_verdict,
        "promotion_allowed": promotion_allowed,
        "flags": flags,
        "key_metrics": {
            "official_decisions": official,
            "watchlist_decisions": watchlist,
            "current_level3_state_ready": current_state_ready,
            "current_level3_trade_ready": current_trade_ready,
            "current_level3_stats_available": current_stats,
            "current_level3_events_available": current_events,
            "historical_statistics_coverage_rate": hist_stats_rate,
            "historical_events_plus_stats_rate": hist_events_stats_rate,
            "signal_settled": signal_settled,
            "signal_roi": signal_roi,
            "clv_5m_tracked": clv_5m_tracked,
            "clv_5m_avg": clv_5m_avg,
            **diag,
        },
    }


def write_markdown(payload: dict[str, Any]) -> None:
    verdict = payload["verdict"]
    flags = verdict["flags"]

    bridge_s = payload["bridge_summary"]
    level3_s = payload["level3_summary"]
    provider_s = payload["provider_summary"]
    research_s = payload["research_summary"]
    settlement_s = payload["settlement_summary"]
    performance_s = payload["performance_summary"]
    clv_s = payload["clv_horizon_summary"]
    diag = payload["research_diagnostics"]

    lines: list[str] = []

    lines += [
        "# FQIS Daily Audit Report",
        "",
        "## Final Verdict",
        "",
        f"- Verdict: **{verdict['final_verdict']}**",
        f"- Promotion allowed: **{verdict['promotion_allowed']}**",
        "",
        "## Flags",
        "",
    ]

    if flags:
        for flag in flags:
            lines.append(f"- {flag}")
    else:
        lines.append("- NONE")

    lines += [
        "",
        "## Current Level 3 Probe",
        "",
        f"- Fixtures inspected: **{level3_s.get('fixtures_inspected', 0)}**",
        f"- State ready: **{level3_s.get('state_ready', level3_s.get('ready_states', 0))}**",
        f"- Trade ready: **{level3_s.get('trade_ready', 0)}**",
        f"- Stats available: **{level3_s.get('stats_available', 0)}**",
        f"- Events available: **{level3_s.get('events_available', 0)}**",
        "",
        "## Production / Bridge",
        "",
        f"- Groups total: **{bridge_s.get('groups_total', 0)}**",
        f"- Groups priced: **{bridge_s.get('groups_priced', 0)}**",
        f"- Groups skipped no Level 3: **{bridge_s.get('groups_skipped_no_level3', 0)}**",
        f"- Decisions total: **{bridge_s.get('decisions_total', 0)}**",
        f"- Official decisions: **{bridge_s.get('official_decisions', 0)}**",
        f"- Watchlist decisions: **{bridge_s.get('watchlist_decisions', 0)}**",
        f"- Blocked decisions: **{bridge_s.get('blocked_decisions', 0)}**",
        "",
        "## Research Screening Diagnostics",
        "",
        f"- Decisions screened: **{diag['decisions_screened']}**",
        f"- Candidates accepted: **{diag['candidates_this_cycle']}**",
        f"- Research acceptance rate: **{pct(diag['research_acceptance_rate'])}**",
        f"- Strict events+stats candidates: **{diag['strict_events_plus_stats']}**",
        f"- Events-only research candidates: **{diag['events_only_research']}**",
        f"- New snapshots appended: **{diag['new_snapshots_appended']}**",
        f"- Rejected by timing policy: **{diag['timing_policy_rejected']}** = **{pct(diag['timing_rejection_rate'])}**",
        f"- Rejected by data tier: **{diag['data_tier_rejected']}** = **{pct(diag['data_tier_rejection_rate'])}**",
        f"- Rejected by non-positive edge/EV: **{diag['non_positive_edge_or_ev_rejected']}** = **{pct(diag['non_positive_edge_or_ev_rejection_rate'])}**",
        f"- Rejected by final status: **{diag['final_status_rejected']}**",
        f"- Rejected by negative-value veto: **{diag['negative_value_veto_rejected']}**",
        "",
        "## Research Side Bias",
        "",
        f"- Research rows total: **{payload.get('research_side_bias', {}).get('research_rows_total', 0)}**",
        f"- UNDER rows: **{payload.get('research_side_bias', {}).get('under_rows', 0)}**",
        f"- OVER rows: **{payload.get('research_side_bias', {}).get('over_rows', 0)}**",
        f"- UNDER share: **{pct(payload.get('research_side_bias', {}).get('under_share', 0))}**",
        f"- OVER share: **{pct(payload.get('research_side_bias', {}).get('over_share', 0))}**",
        f"- Dominant side: **{payload.get('research_side_bias', {}).get('dominant_side', '')}**",
        "",
        "## Historical Provider Coverage",
        "",
        "> This section is historical over stored raw Level 3 files, not only the latest cycle.",
        "",
        f"- Fixtures total: **{provider_s.get('fixtures_total', 0)}**",
        f"- Events coverage rate: **{pct(provider_s.get('events_coverage_rate', 0))}**",
        f"- Statistics coverage rate: **{pct(provider_s.get('statistics_coverage_rate', 0))}**",
        f"- Events + stats rate: **{pct(provider_s.get('events_plus_stats_rate', 0))}**",
        "",
        "## Research Settlement",
        "",
        f"- Rows total: **{settlement_s.get('rows_total', 0)}**",
        f"- Settled: **{settlement_s.get('settled', 0)}**",
        f"- Wins: **{settlement_s.get('wins', 0)}**",
        f"- Losses: **{settlement_s.get('losses', 0)}**",
        f"- PnL unit total: **{settlement_s.get('pnl_unit_total', 0)}**",
        f"- ROI unit: **{settlement_s.get('roi_unit', 0)}**",
        "",
        "## Research Performance",
        "",
        "| Level | Rows | Settled | PnL | ROI | Avg CLV |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for level in ["snapshot", "signal", "match"]:
        m = performance_s.get(level) or {}
        lines.append(
            f"| {level.upper()} | {m.get('rows', 0)} | {m.get('settled', 0)} | {m.get('pnl_unit', 0)} | {m.get('roi_unit', 0)} | {m.get('avg_clv_decimal', '')} |"
        )

    lines += [
        "",
        "## Fixed-Horizon CLV",
        "",
        "| Horizon | Tracked | Positive | Negative | Avg CLV | Positive rate |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    horizons = clv_s.get("horizons") or {}
    for horizon in ["1m", "5m", "15m", "near_close"]:
        h = horizons.get(horizon) or {}
        lines.append(
            f"| {horizon} | {h.get('tracked', 0)} | {h.get('positive', 0)} | {h.get('negative', 0)} | {pct(h.get('avg', 0))} | {pct(h.get('positive_rate', 0))} |"
        )

    lines += [
        "",
        "## Institutional Read",
        "",
        "Current system state: production is protected, no promotion is allowed, and current-cycle Level 3 quality plus research sample size remain below institutional thresholds.",
        "",
        "Next target: accumulate more samples while tracking rejection rates, fixed-horizon CLV, strict events+stats candidates, settlement, and bucket stability.",
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    bridge = read_json(BRIDGE_JSON)
    level3 = read_json(LEVEL3_JSON)
    research = read_json(RESEARCH_JSON)
    settlement = read_json(SETTLEMENT_JSON)
    performance = read_json(PERFORMANCE_JSON)
    clv = read_json(CLV_HORIZON_JSON)
    provider = read_json(PROVIDER_JSON)

    bridge_s = summary(bridge)
    level3_s = summary(level3)
    research_s = summary(research)
    settlement_s = summary(settlement)
    performance_s = summary(performance)
    clv_s = summary(clv)
    provider_s = summary(provider)

    diag = research_diagnostics(research)
    side_bias = research_side_bias_metrics()

    verdict = build_verdict(
        bridge_s=bridge_s,
        level3_s=level3_s,
        provider_s=provider_s,
        perf_s=performance_s,
        clv_s=clv_s,
        diag=diag,
    )

    if side_bias["research_rows_total"] >= 50 and side_bias["under_share"] >= 0.70:
        verdict["flags"].append("RESEARCH_SIDE_BIAS_UNDER_DOMINANT")
        verdict["key_metrics"].update(side_bias)

    if side_bias["research_rows_total"] >= 50 and side_bias["over_share"] >= 0.70:
        verdict["flags"].append("RESEARCH_SIDE_BIAS_OVER_DOMINANT")
        verdict["key_metrics"].update(side_bias)

    if "research_side_bias" not in verdict["key_metrics"]:
        verdict["key_metrics"].update(side_bias)

    payload = {
        "mode": "FQIS_DAILY_AUDIT_REPORT",
        "generated_at_utc": utc_now(),
        "verdict": verdict,
        "bridge_summary": bridge_s,
        "level3_summary": level3_s,
        "research_summary": research_s,
        "research_diagnostics": diag,
        "research_side_bias": side_bias,
        "settlement_summary": settlement_s,
        "performance_summary": performance_s,
        "clv_horizon_summary": clv_s,
        "provider_summary": provider_s,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)

    print(json.dumps({
        "status": "READY",
        "verdict": verdict,
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
