from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

SOURCES = {
    "live_decisions": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json",
    "operator": ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_operator_report.json",
    "level3_current": ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_level3_live_state.json",
    "research_perf": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.json",
    "settlement": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json",
    "clv_horizon": ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.json",
    "provider_historical": ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage" / "latest_provider_coverage_report.json",
}

OUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "audit"
OUT_JSON = OUT_DIR / "latest_daily_audit_report.json"
OUT_MD = OUT_DIR / "latest_daily_audit_report.md"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def get(d: dict[str, Any], *keys: str, default: Any = 0) -> Any:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def verdict(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    live = payloads["live_decisions"]
    level3 = payloads["level3_current"]
    perf = payloads["research_perf"]
    provider = payloads["provider_historical"]
    clv = payloads["clv_horizon"]

    live_summary = live.get("summary") or {}
    level3_summary = level3.get("summary") or {}
    perf_summary = get(perf, "summary", default={})
    provider_summary = provider.get("summary") or {}
    horizons = get(clv, "summary", "horizons", default={})

    official = int(live_summary.get("official_decisions") or 0)
    watchlist = int(live_summary.get("watchlist_decisions") or 0)

    current_level3_fixtures = int(level3_summary.get("fixtures_inspected") or 0)
    current_level3_state_ready = int(level3_summary.get("state_ready") or level3_summary.get("ready_states") or 0)
    current_level3_trade_ready = int(level3_summary.get("trade_ready") or 0)
    current_level3_stats_available = int(level3_summary.get("stats_available") or 0)
    current_level3_events_available = int(level3_summary.get("events_available") or 0)

    historical_stats_rate = float(provider_summary.get("statistics_coverage_rate") or 0.0)
    historical_events_rate = float(provider_summary.get("events_coverage_rate") or 0.0)
    historical_events_plus_stats_rate = float(provider_summary.get("events_plus_stats_rate") or 0.0)

    signal_settled = int(get(perf_summary, "signal", "settled", default=0) or 0)
    signal_roi = float(get(perf_summary, "signal", "roi_unit", default=0.0) or 0.0)

    clv_5m_avg = float(get(horizons, "5m", "avg", default=0.0) or 0.0)
    clv_5m_tracked = int(get(horizons, "5m", "tracked", default=0) or 0)

    flags = []

    if official > 0 or watchlist > 0:
        flags.append("PUBLISHABLE_DECISIONS_EXIST_REVIEW_REQUIRED")
    else:
        flags.append("NO_PUBLISHABLE_DECISION")

    if current_level3_fixtures == 0:
        flags.append("NO_CURRENT_LEVEL3_FIXTURE_INSPECTED")

    if current_level3_state_ready == 0:
        flags.append("NO_CURRENT_LEVEL3_STATE_READY")

    if current_level3_trade_ready == 0:
        flags.append("NO_CURRENT_LEVEL3_TRADE_READY_FIXTURE")

    if current_level3_fixtures > 0 and current_level3_events_available == 0:
        flags.append("CURRENT_LEVEL3_EVENTS_EMPTY")

    if current_level3_fixtures > 0 and current_level3_stats_available == 0:
        flags.append("CURRENT_LEVEL3_STATS_EMPTY")

    if historical_stats_rate < 0.25:
        flags.append("LOW_HISTORICAL_LIVE_STATISTICS_COVERAGE")

    if signal_settled < 100:
        flags.append("INSUFFICIENT_RESEARCH_SAMPLE")

    if clv_5m_tracked == 0:
        flags.append("NO_FIXED_HORIZON_CLV")
    elif clv_5m_avg <= 0:
        flags.append("CLV_5M_NOT_POSITIVE")

    promotion_allowed = (
        official == 0
        and watchlist == 0
        and signal_settled >= 1000
        and signal_roi >= 0
        and clv_5m_tracked >= 500
        and clv_5m_avg > 0
        and historical_stats_rate >= 0.50
        and current_level3_trade_ready > 0
    )

    final = "PROMOTION_REVIEW_POSSIBLE" if promotion_allowed else "NO_PROMOTION_KEEP_RESEARCH"

    return {
        "final_verdict": final,
        "flags": flags,
        "promotion_allowed": promotion_allowed,
        "key_metrics": {
            "official_decisions": official,
            "watchlist_decisions": watchlist,
            "current_level3_fixtures": current_level3_fixtures,
            "current_level3_state_ready": current_level3_state_ready,
            "current_level3_trade_ready": current_level3_trade_ready,
            "current_level3_stats_available": current_level3_stats_available,
            "current_level3_events_available": current_level3_events_available,
            "historical_events_coverage_rate": historical_events_rate,
            "historical_statistics_coverage_rate": historical_stats_rate,
            "historical_events_plus_stats_rate": historical_events_plus_stats_rate,
            "signal_settled": signal_settled,
            "signal_roi": signal_roi,
            "clv_5m_tracked": clv_5m_tracked,
            "clv_5m_avg": clv_5m_avg,
        },
    }


def write_markdown(payload: dict[str, Any]) -> None:
    v = payload["verdict"]
    p = payload["payloads"]

    live_summary = p["live_decisions"].get("summary") or {}
    level3_summary = p["level3_current"].get("summary") or {}
    provider_summary = p["provider_historical"].get("summary") or {}
    settlement_summary = p["settlement"].get("summary") or {}
    perf_summary = get(p["research_perf"], "summary", default={})
    horizons = get(p["clv_horizon"], "summary", "horizons", default={})

    lines = [
        "# FQIS Daily Audit Report",
        "",
        "## Final Verdict",
        "",
        f"- Verdict: **{v['final_verdict']}**",
        f"- Promotion allowed: **{v['promotion_allowed']}**",
        "",
        "## Flags",
        "",
    ]

    for flag in v["flags"]:
        lines.append(f"- {flag}")

    lines += [
        "",
        "## Current Level 3 Probe",
        "",
        f"- Fixtures inspected: **{level3_summary.get('fixtures_inspected', 0)}**",
        f"- State ready: **{level3_summary.get('state_ready', level3_summary.get('ready_states', 0))}**",
        f"- Trade ready: **{level3_summary.get('trade_ready', 0)}**",
        f"- Stats available: **{level3_summary.get('stats_available', 0)}**",
        f"- Events available: **{level3_summary.get('events_available', 0)}**",
        "",
        "## Production / Bridge",
        "",
        f"- Groups total: **{live_summary.get('groups_total', 0)}**",
        f"- Groups priced: **{live_summary.get('groups_priced', 0)}**",
        f"- Groups skipped no Level 3: **{live_summary.get('groups_skipped_no_level3', 0)}**",
        f"- Decisions total: **{live_summary.get('decisions_total', 0)}**",
        f"- Official decisions: **{live_summary.get('official_decisions', 0)}**",
        f"- Watchlist decisions: **{live_summary.get('watchlist_decisions', 0)}**",
        f"- Blocked decisions: **{live_summary.get('blocked_decisions', 0)}**",
        "",
        "## Historical Provider Coverage",
        "",
        "> This section is historical over stored raw Level 3 files, not only the latest cycle.",
        "",
        f"- Fixtures total: **{provider_summary.get('fixtures_total', 0)}**",
        f"- Events coverage rate: **{float(provider_summary.get('events_coverage_rate') or 0):.2%}**",
        f"- Statistics coverage rate: **{float(provider_summary.get('statistics_coverage_rate') or 0):.2%}**",
        f"- Events + stats rate: **{float(provider_summary.get('events_plus_stats_rate') or 0):.2%}**",
        "",
        "## Research Settlement",
        "",
        f"- Rows total: **{settlement_summary.get('rows_total', 0)}**",
        f"- Settled: **{settlement_summary.get('settled', 0)}**",
        f"- Wins: **{settlement_summary.get('wins', 0)}**",
        f"- Losses: **{settlement_summary.get('losses', 0)}**",
        f"- PnL unit total: **{settlement_summary.get('pnl_unit_total', 0)}**",
        f"- ROI unit: **{settlement_summary.get('roi_unit', 0)}**",
        "",
        "## Research Performance",
        "",
        "| Level | Rows | Settled | PnL | ROI | Avg CLV |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for level in ["snapshot", "signal", "match"]:
        m = get(perf_summary, level, default={})
        avg_clv = m.get("avg_clv_decimal")
        avg_clv_text = "" if avg_clv is None else str(avg_clv)
        lines.append(
            f"| {level.upper()} | {m.get('rows', 0)} | {m.get('settled', 0)} | {m.get('pnl_unit', 0)} | {m.get('roi_unit', 0)} | {avg_clv_text} |"
        )

    lines += [
        "",
        "## Fixed-Horizon CLV",
        "",
        "| Horizon | Tracked | Positive | Negative | Avg CLV | Positive rate |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for h in ["1m", "5m", "15m", "near_close"]:
        m = horizons.get(h) or {}
        lines.append(
            f"| {h} | {m.get('tracked', 0)} | {m.get('positive', 0)} | {m.get('negative', 0)} | {float(m.get('avg') or 0):.2%} | {float(m.get('positive_rate') or 0):.2%} |"
        )

    lines += [
        "",
        "## Institutional Read",
        "",
        "Current system state: production is protected, no promotion is allowed, and current-cycle Level 3 quality is not sufficient for publication.",
        "",
        "Next target: accumulate more research samples, separate strict events+stats from events-only research, and improve current-cycle Level 3 trade-ready coverage.",
    ]

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payloads = {name: read_json(path) for name, path in SOURCES.items()}

    payload = {
        "mode": "FQIS_DAILY_AUDIT_REPORT_V2",
        "generated_at_utc": utc_now(),
        "sources": {name: str(path) for name, path in SOURCES.items()},
        "payloads": payloads,
        "verdict": verdict(payloads),
    }

    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)

    print(json.dumps({
        "status": "READY",
        "verdict": payload["verdict"],
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
