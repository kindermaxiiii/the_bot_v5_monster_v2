from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_operator_report.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_operator_report.json"


def fnum(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def pct(x: Any) -> str:
    return f"{fnum(x) * 100:.2f}%"


def load_payload() -> dict[str, Any]:
    return json.loads(SOURCE.read_text(encoding="utf-8"))


def blocker_family(veto: str) -> str:
    v = str(veto or "")

    if v.startswith("level3_"):
        return "LEVEL3_DATA"

    if v == "non_positive_edge":
        return "NEGATIVE_VALUE"

    if is_market_research_veto(v):
        return "MARKET_STRUCTURE"

    if "edge" in v:
        return "EDGE"

    if "ev" in v:
        return "EV"

    if "regime" in v or "confidence" in v or "fragility" in v:
        return "EXECUTION_RISK"

    return "OTHER"


def is_market_research_veto(veto: str) -> bool:
    v = str(veto or "")
    return (
        "under_0_5" in v
        or "under_1_5" in v
        or "under_2_5" in v
        or "under_one_goal" in v
        or "under_two_goal" in v
        or "under_goal_budget" in v
    )


def decision_key(d: dict[str, Any]) -> tuple[Any, ...]:
    return (
        d.get("fixture_id"),
        d.get("match"),
        d.get("score"),
        d.get("minute"),
        d.get("selection"),
        round(fnum(d.get("odds_decimal")), 3),
        round(fnum(d.get("edge")), 6),
        round(fnum(d.get("expected_value")), 6),
    )


def dedupe_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []

    for d in rows:
        key = decision_key(d)
        if key in seen:
            continue
        seen.add(key)
        out.append(d)

    return out


def has_level3_veto(d: dict[str, Any]) -> bool:
    return any(str(v).startswith("level3_") for v in d.get("vetoes") or [])


def has_market_research_veto(d: dict[str, Any]) -> bool:
    return any(is_market_research_veto(v) for v in d.get("vetoes") or [])


def has_negative_value_veto(d: dict[str, Any]) -> bool:
    vetoes = set(str(v) for v in d.get("vetoes") or [])
    return "non_positive_edge" in vetoes



def operational_final_status(d: dict[str, Any]) -> str:
    vetoes = d.get("vetoes") or []
    if vetoes:
        return "NO_BET"
    return str(d.get("real_status") or "NO_BET")


def main() -> int:
    payload = load_payload()
    raw_decisions = payload.get("decisions") or []
    decisions = dedupe_decisions(raw_decisions)
    summary = payload.get("summary") or {}

    veto_counter = Counter()
    family_counter = Counter()
    l3_mode_counter = Counter()
    by_match = defaultdict(list)

    for d in decisions:
        p = d.get("payload") or {}
        l3_mode_counter[str(p.get("level3_data_mode") or "NA")] += 1
        by_match[str(d.get("match") or "UNKNOWN")].append(d)

        for veto in d.get("vetoes") or []:
            veto_counter[str(veto)] += 1
            family_counter[blocker_family(str(veto))] += 1

    trade_ready = [
        d for d in decisions
        if (d.get("payload") or {}).get("level3_trade_ready") is True
    ]

    positive_trade_ready = [
        d for d in trade_ready
        if fnum(d.get("edge")) > 0 and fnum(d.get("expected_value")) > 0
    ]

    positive_trade_ready.sort(
        key=lambda d: (fnum(d.get("expected_value")), fnum(d.get("edge"))),
        reverse=True,
    )

    market_research = [
        d for d in positive_trade_ready
        if has_market_research_veto(d)
    ]

    true_near_pass = [
        d for d in positive_trade_ready
        if not has_level3_veto(d)
        and not has_market_research_veto(d)
        and not has_negative_value_veto(d)
        and not (d.get("vetoes") or [])
    ]

    market_research.sort(
        key=lambda d: (fnum(d.get("expected_value")), fnum(d.get("edge"))),
        reverse=True,
    )

    true_near_pass.sort(
        key=lambda d: (fnum(d.get("expected_value")), fnum(d.get("edge"))),
        reverse=True,
    )

    match_rows = []
    for match, rows in by_match.items():
        best = max(rows, key=lambda d: fnum(d.get("expected_value")))
        p = best.get("payload") or {}

        match_rows.append({
            "match": match,
            "decisions": len(rows),
            "best_selection": best.get("selection"),
            "best_ev": fnum(best.get("expected_value")),
            "best_edge": fnum(best.get("edge")),
            "l3_mode": p.get("level3_data_mode"),
            "l3_trade": bool(p.get("level3_trade_ready")),
            "pre_veto_status": p.get("pre_external_veto_real_status", best.get("real_status")),
            "final_status": operational_final_status(best),
            "main_veto": (best.get("vetoes") or [""])[0],
        })

    match_rows.sort(key=lambda r: (r["best_ev"], r["best_edge"]), reverse=True)

    report = {
        "summary": summary,
        "raw_decisions_total": len(raw_decisions),
        "deduped_decisions_total": len(decisions),
        "top_vetoes": veto_counter.most_common(20),
        "blocker_families": family_counter.most_common(),
        "level3_modes": l3_mode_counter.most_common(),
        "trade_ready_count": len(trade_ready),
        "positive_trade_ready_count": len(positive_trade_ready),
        "true_near_pass_count": len(true_near_pass),
        "market_research_count": len(market_research),
        "true_near_pass": true_near_pass[:20],
        "market_research": market_research[:20],
        "match_rows": match_rows[:30],
    }

    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    lines = []
    lines.append("# FQIS Operator Decision Report")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- Raw decisions total: **{len(raw_decisions)}**")
    lines.append(f"- Deduped decisions total: **{len(decisions)}**")
    lines.append(f"- Official decisions: **{summary.get('official_decisions', 0)}**")
    lines.append(f"- Watchlist decisions: **{summary.get('watchlist_decisions', 0)}**")
    lines.append(f"- Blocked decisions: **{summary.get('blocked_decisions', 0)}**")
    lines.append(f"- Groups total: **{summary.get('groups_total', 0)}**")
    lines.append(f"- Groups priced: **{summary.get('groups_priced', 0)}**")
    lines.append(f"- Groups skipped no Level 3: **{summary.get('groups_skipped_no_level3', 0)}**")
    lines.append(f"- Level 3 state ready: **{summary.get('level3_state_ready', 0)}**")
    lines.append(f"- Level 3 trade ready: **{summary.get('level3_trade_ready', 0)}**")
    lines.append(f"- Level 3 stats available: **{summary.get('level3_stats_available', 0)}**")
    lines.append(f"- Level 3 events available: **{summary.get('level3_events_available', 0)}**")
    lines.append(f"- Trade-ready positive EV: **{len(positive_trade_ready)}**")
    lines.append(f"- True near-pass: **{len(true_near_pass)}**")
    lines.append(f"- Market-research candidates: **{len(market_research)}**")
    lines.append("")

    lines.append("## Interpretation")
    lines.append("")
    if summary.get("official_decisions", 0) == 0 and summary.get("watchlist_decisions", 0) == 0:
        lines.append("No publishable decision. The system is blocking correctly.")
    else:
        lines.append("There are publishable decisions. Review them before any operational escalation.")
    lines.append("")
    lines.append("Market-research candidates are not close to live publication. They are candidates for settlement, CLV tracking, and doctrine review only.")
    lines.append("")

    lines.append("## Blocker Families")
    lines.append("")
    lines.append("| Family | Count |")
    lines.append("|---|---:|")
    for family, count in family_counter.most_common():
        lines.append(f"| {family} | {count} |")

    lines.append("")
    lines.append("## Top Vetoes")
    lines.append("")
    lines.append("| Veto | Count |")
    lines.append("|---|---:|")
    for veto, count in veto_counter.most_common(20):
        lines.append(f"| {veto} | {count} |")

    lines.append("")
    lines.append("## Level 3 Modes")
    lines.append("")
    lines.append("| Mode | Count |")
    lines.append("|---|---:|")
    for mode, count in l3_mode_counter.most_common():
        lines.append(f"| {mode} | {count} |")

    lines.append("")
    lines.append("## Trade-Ready Positive EV Candidates")
    lines.append("")
    if not positive_trade_ready:
        lines.append("No trade-ready positive EV candidate.")
    else:
        lines.append("| Match | Score | Min | Selection | Odds | Edge | EV | Pre-veto | Final | Vetoes |")
        lines.append("|---|---:|---:|---|---:|---:|---:|---|---|---|")
        for d in positive_trade_ready[:20]:
            vetoes = ", ".join(d.get("vetoes") or [])
            p = d.get("payload") or {}
            lines.append(
                "| {match} | {score} | {minute} | {selection} | {odds:.3f} | {edge} | {ev} | {pre} | {final} | {vetoes} |".format(
                    match=str(d.get("match", "")).replace("|", "/"),
                    score=d.get("score", ""),
                    minute=d.get("minute", ""),
                    selection=d.get("selection", ""),
                    odds=fnum(d.get("odds_decimal")),
                    edge=pct(d.get("edge")),
                    ev=pct(d.get("expected_value")),
                    pre=p.get("pre_external_veto_real_status", d.get("real_status", "")),
                    final=operational_final_status(d),
                    vetoes=vetoes.replace("|", "/"),
                )
            )

    lines.append("")
    lines.append("## Market-Research Candidates")
    lines.append("")
    lines.append("These are trade-ready and positive EV, but blocked by market doctrine. They are research candidates only.")
    lines.append("")
    if not market_research:
        lines.append("No market-research candidate.")
    else:
        lines.append("| Match | Score | Min | Selection | Odds | Edge | EV | Pre-veto | Final | Vetoes |")
        lines.append("|---|---:|---:|---|---:|---:|---:|---|---|---|")
        for d in market_research[:20]:
            vetoes = ", ".join(d.get("vetoes") or [])
            p = d.get("payload") or {}
            lines.append(
                "| {match} | {score} | {minute} | {selection} | {odds:.3f} | {edge} | {ev} | {pre} | {final} | {vetoes} |".format(
                    match=str(d.get("match", "")).replace("|", "/"),
                    score=d.get("score", ""),
                    minute=d.get("minute", ""),
                    selection=d.get("selection", ""),
                    odds=fnum(d.get("odds_decimal")),
                    edge=pct(d.get("edge")),
                    ev=pct(d.get("expected_value")),
                    pre=p.get("pre_external_veto_real_status", d.get("real_status", "")),
                    final=operational_final_status(d),
                    vetoes=vetoes.replace("|", "/"),
                )
            )

    lines.append("")
    lines.append("## Closest To Passing")
    lines.append("")
    if not true_near_pass:
        lines.append("No true near-pass candidate. Current blockers are hard vetoes: Level 3 data, market doctrine, edge/EV floor, negative value, or execution risk.")
    else:
        lines.append("| Match | Score | Min | Selection | Odds | Edge | EV | Pre-veto | Final |")
        lines.append("|---|---:|---:|---|---:|---:|---:|---|---|")
        for d in true_near_pass[:15]:
            p = d.get("payload") or {}
            lines.append(
                "| {match} | {score} | {minute} | {selection} | {odds:.3f} | {edge} | {ev} | {pre} | {final} |".format(
                    match=str(d.get("match", "")).replace("|", "/"),
                    score=d.get("score", ""),
                    minute=d.get("minute", ""),
                    selection=d.get("selection", ""),
                    odds=fnum(d.get("odds_decimal")),
                    edge=pct(d.get("edge")),
                    ev=pct(d.get("expected_value")),
                    pre=p.get("pre_external_veto_real_status", d.get("real_status", "")),
                    final=operational_final_status(d),
                )
            )

    lines.append("")
    lines.append("## Best Candidate By Match")
    lines.append("")
    lines.append("| Match | Decisions | Best selection | Best EV | Best edge | L3 mode | L3 trade | Pre-veto | Final | Main veto |")
    lines.append("|---|---:|---|---:|---:|---|---|---|---|---|")
    for r in match_rows[:30]:
        lines.append(
            "| {match} | {decisions} | {best_selection} | {best_ev} | {best_edge} | {l3_mode} | {l3_trade} | {pre_veto} | {final_status} | {main_veto} |".format(
                match=str(r["match"]).replace("|", "/"),
                decisions=r["decisions"],
                best_selection=r["best_selection"],
                best_ev=pct(r["best_ev"]),
                best_edge=pct(r["best_edge"]),
                l3_mode=r["l3_mode"],
                l3_trade="yes" if r["l3_trade"] else "no",
                pre_veto=r["pre_veto_status"],
                final_status=r["final_status"],
                main_veto=r["main_veto"],
            )
        )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": "READY",
        "operator_report_md": str(OUT_MD),
        "operator_report_json": str(OUT_JSON),
        "raw_decisions": len(raw_decisions),
        "deduped_decisions": len(decisions),
        "trade_ready": len(trade_ready),
        "positive_trade_ready": len(positive_trade_ready),
        "true_near_pass": len(true_near_pass),
        "market_research": len(market_research),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
